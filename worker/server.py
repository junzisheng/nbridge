import asyncio
from collections import defaultdict
import os
from typing import Tuple, Dict, Optional, List, Type
from multiprocessing.connection import Connection
from functools import partial
from asyncio.base_events import Server
from asyncio import Task, CancelledError, TimeoutError, Future
from async_timeout import timeout

from loguru import logger

from constants import CloseReason
from worker.bases import ProcessWorker, ProxyStateWrapper, ClientStruct, ProxyPool
from aexit_context import AexitContext
from messager import Message, Event, ProcessPipeMessageKeeper, SocketChannel
from public.protocol import PublicProtocol
from utils import safe_remove, catch_cor_exception, socket_fromfd, protocol_sockname
from config.settings import server_settings


@catch_cor_exception
async def _proxy_public_pair(client: ClientStruct, public: PublicProtocol, local_addr: Tuple[str, int], delay: int) -> None:
    try:
        async with timeout(delay):
            proxy = await client.proxy_pool.get()  # type: ProxyStateWrapper
    except CancelledError:
        public.transport.close()
    except TimeoutError:
        public.transport.close()
    except Exception as e:
        logger.exception(e)  # unexpected
        public.transport.close()
    else:
        proxy.reset_forwarder()
        proxy.set_forwarder(public, *local_addr)
        public.set_forwarder(proxy)


class ManagerWorker(ProcessWorker):
    def __init__(
        self, keeper_cls: Type[ProcessPipeMessageKeeper], input_channel: Connection,
        output_channel: Connection, socket_channel: Connection
    ) -> None:
        super().__init__(keeper_cls, input_channel, output_channel)
        self.client_info: Optional[Dict[str, ClientStruct]] = {}
        self.proxy_port: Optional[int] = None
        self.socket_receiver = SocketChannel(socket_channel, self.pid)

    def async_create_proxy(self, client_name: str, num: int = 1) -> None:
        self.message_keeper.send(Message(Event.PROXY_CREATE, client_name, self.proxy_port, num))

    # def initialize(self) -> None:
    #     for client_name, client in server_settings.client_map.items():
    #         self.add_client(client_name, client.token)

    def start(self) -> None:
        # self.initialize()
        self.run_proxy_server()

        @self.aexit.add_callback_when_cancel_all
        def _():
            for client in self.client_info.values():
                client.close_session()

    async def _handle_stop(self) -> None:
        waiters = []
        for client in self.client_info.values():
            waiters.append(client.destroy())
        await asyncio.gather(*waiters)

    def run_proxy_server(self) -> None:
        def on_proxy_session_made(proxy: ProxyStateWrapper) -> None:
            client = self.client_info.get(proxy.client_name)
            if not client or not client.session:
                proxy.close(CloseReason.MANAGE_CLIENT_LOST)
            else:
                client.proxy_pool.put_nowait(proxy)

        def on_proxy_session_lost(proxy: ProxyStateWrapper) -> None:
            # if proxy.close_reason not in [CloseReason.PROXY_RECYCLE, CloseReason.MANAGE_CLIENT_LOST]:
            client = self.client_info[proxy.client_name]
            client.proxy_pool.remove(proxy)

        def on_task_done(proxy: ProxyStateWrapper):
            client = self.client_info.get(proxy.client_name)
            if client and client.session:
                client.proxy_pool.put_nowait(proxy)

        proxy_server: Server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(
                    ProxyStateWrapper, self.client_info, on_proxy_session_made, on_proxy_session_lost, on_task_done
                ),
                host=server_settings.manager_bind_host,
                port=0
            )
        )
        self.proxy_port = proxy_server.sockets[0].getsockname()[1]
        self.message_keeper.put(self.proxy_port)
        self.aexit.add_callback_when_cancel_all(proxy_server.close)

    def make_receiver(self) -> Dict:
        client_aexit_map: Dict[str, AexitContext] = defaultdict(AexitContext)
        tcp_aexit_map: Dict[int, AexitContext] = defaultdict(AexitContext)

        @self.aexit.add_callback_when_cancel_all
        def _():
            for aexit in list(client_aexit_map.values()) + list(tcp_aexit_map.values()):
                aexit.cancel_all()
            for prt in public_protocol_list:
                prt.transport.close()
            for ps, _, _ in public_server:
                ps.close()

        def manager_session_made(client_name: str, epoch) -> None:
            client = self.client_info[client_name]
            client.open_session(epoch)

        def manager_session_lost(client_name: str) -> Future:
            client = self.client_info[client_name]
            client_aexit_map[client_name].cancel_all()
            # _close_public_server(client_name=client_name)
            # wait all protocol closed callback
            return client.close_session()

        # =============== commander ===============
        def client_add(client_name: str, token: str) -> None:
            proxy_pool = ProxyPool(
                self._loop,
                partial(self.async_create_proxy, client_name),
                min_size=server_settings.proxy_pool_size
            )
            client = ClientStruct(
                name=client_name, token=token,
                proxy_pool=proxy_pool
            )
            self.client_info[client_name] = client
            proxy_pool.start_recycle_task()
            client_aexit_map[client_name].add_callback_when_cancel_all(proxy_pool.recycle_task.cancel)

        def client_remove(client_name: str) -> Future:
            client = self.client_info[client_name]
            f = client.close_session()

            @f.add_done_callback
            def _(_):
                del self.client_info[client_name]
                del client_aexit_map[client_name]
            client_aexit_map[client_name].cancel_all()
            _close_public_server(client_name=client_name)
            return f

        # public proxy pair
        pair_tasks: Dict[PublicProtocol, Task] = {}
        public_server: List[Tuple[Server, str, int]] = []
        public_protocol_list: List[PublicProtocol] = []

        # close public server by port or client_name
        def _close_public_server(client_name: Optional[str] = None, port: Optional[int] = None) -> None:
            remove_list: List[Tuple[Server, str, int]] = []
            for ps in public_server:
                if client_name and ps[1] == client_name or port and ps[2] == port:
                    remove_list.append(ps)
            for ps in remove_list:
                public_server.remove(ps)
                ps[0].close()

        def tcp_add(client_name: str, local_addr: Tuple[str, int]) -> None:
            fileno = self.socket_receiver.receive()
            sock = socket_fromfd(fileno)
            os.close(fileno)  # close duplicate fd
            port = sock.getsockname()[1]

            def on_connection_made(public: PublicProtocol) -> None:
                _client = self.client_info.get(client_name)
                if not _client or not _client.session:  # may be client lost or removed
                    public.transport.close()
                else:
                    public_protocol_list.append(public)
                    task = client_aexit_map[client_name].create_task(
                        _proxy_public_pair(_client, public, local_addr, server_settings.proxy_wait_timeout)
                    )
                    pair_tasks[public] = task
                    task.add_done_callback(lambda _x: safe_remove(pair_tasks,  public))
                    tcp_aexit_map[port].monitor_future(task)

            def on_connection_lost(public: PublicProtocol) -> None:
                safe_remove(public_protocol_list, public)
                task = pair_tasks.get(public)
                if task:  # cancel pair task
                    task.cancel()

            async def create_tcp_server():
                server: Server = await self._loop.create_server(
                    partial(PublicProtocol, on_connection_made, on_connection_lost),
                    sock=sock
                )
                public_server.append((server, client_name, sock.getsockname()[1]))

            t = tcp_aexit_map[port].create_task(create_tcp_server())
            client_aexit_map[client_name].monitor_future(t)

        def tcp_remove(port: int) -> None:
            for public in public_protocol_list:
                if protocol_sockname(public.transport)[1] == port:
                    public.transport.close()
            _close_public_server(port=port)
            tcp_aexit_map[port].cancel_all()
            del tcp_aexit_map[port]

        return {
            Event.MANAGER_SESSION_MADE: manager_session_made,
            Event.MANAGER_SESSION_LOST: manager_session_lost,
            Event.CLIENT_ADD: client_add,
            Event.CLIENT_REMOVE: client_remove,
            Event.TCP_ADD: tcp_add,
            Event.TCP_REMOVE: tcp_remove,
        }






