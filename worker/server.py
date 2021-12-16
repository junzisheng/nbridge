import asyncio
from typing import Tuple, Dict, Optional, List, Type, Union
from multiprocessing.connection import Connection
from functools import partial
from asyncio.base_events import Server
from asyncio import Task, CancelledError, TimeoutError, Future
import socket
from async_timeout import timeout

from loguru import logger

from constants import CloseReason
from worker.bases import ProcessWorker, ProxyStateWrapper, ClientStruct, ProxyPool
from messager import Message, Event, ProcessPipeMessageKeeper
from public.protocol import PublicProtocol
from utils import safe_remove, catch_cor_exception
from config.settings import server_settings


@catch_cor_exception
async def _proxy_public_pair(client: ClientStruct, public: PublicProtocol, delay: int) -> None:
    try:
        async with timeout(delay):
            proxy = await client.proxy_pool.get()  # type: ProxyStateWrapper
            print(proxy.state, proxy._id)
    except CancelledError:
        public.transport.close()
    except TimeoutError:
        public.transport.close()
    except Exception as e:
        logger.exception(e)  # unexpected
        public.transport.close()
    else:
        proxy.reset_forwarder()
        proxy.set_forwarder(public)
        public.set_forwarder(proxy)


class ManagerWorker(ProcessWorker):
    def __init__(
        self, keeper_cls: Type[ProcessPipeMessageKeeper], input_channel: Connection,
        output_channel: Connection, public_servers: Dict[str, List[socket.socket]]
    ) -> None:
        super().__init__(keeper_cls, input_channel, output_channel)
        self.public_servers = public_servers
        self.client_info: Optional[Dict[str, ClientStruct]] = None
        self.proxy_port: Optional[int] = None

    def async_create_proxy(self, client_name: str, num: int = 1) -> None:
        self.message_keeper.send(Message(Event.PROXY_CREATE, client_name, self.proxy_port, num))

    def initialize(self) -> None:
        client_info: Dict[str, ClientStruct] = {}
        for client_name, client in server_settings.client_map.items():
            proxy_pool = ProxyPool(
                self._loop,
                partial(self.async_create_proxy, client_name),
                min_size=server_settings.proxy_pool_size
            )
            proxy_pool.start_recycle_task()
            client_info[client_name] = ClientStruct(
                name=client_name, token=client.token,
                public_sockets=self.public_servers[client_name],
                proxy_pool=proxy_pool
            )

            @self.aexit.add_callback_when_cancel_all
            def _():
                proxy_pool.recycle_task.cancel()
                client_info[client_name].close_session()
        self.client_info = client_info

    def start(self) -> None:
        self.initialize()
        self.run_proxy_server()
        self.run_public()

        @self.aexit.add_callback_when_cancel_all
        def _():
            for client in self.client_info.values():
                client.close_session()

    async def _handle_stop(self) -> None:
        # for client in self.client_info.values():
        #     await client.close_session()
        await asyncio.sleep(0)

    def run_public(self) -> None:
        # 配对任务
        pair_tasks: Dict[PublicProtocol, Task] = {}
        for client_name, sock_list in self.public_servers.items():
            client = self.client_info[client_name]

            def on_connection_made(_client: ClientStruct, public: PublicProtocol) -> None:
                if not _client.session:
                    public.transport.close()
                else:
                    task = _client.aexit.create_task(
                        _proxy_public_pair(_client, public, server_settings.proxy_wait_timeout)
                    )
                    pair_tasks[public] = task
                    task.add_done_callback(lambda _x: safe_remove(pair_tasks,  public))

            def on_connection_lost(public: PublicProtocol) -> None:
                task = pair_tasks.get(public)
                if task:
                    task.cancel()

            for sock in sock_list:
                server: Server = self._loop.run_until_complete(
                    self._loop.create_server(
                        partial(PublicProtocol, partial(on_connection_made, client), on_connection_lost),
                        sock=sock
                    )
                )
                client.aexit.add_callback_when_cancel_all(server.close)

    def run_proxy_server(self) -> None:
        def on_proxy_session_made(proxy: ProxyStateWrapper) -> None:
            client = self.client_info[proxy.client_name]
            if not client.session:
                proxy.close(CloseReason.MANAGE_CLIENT_LOST)
            else:
                client.proxy_pool.put_nowait(proxy)

        def on_proxy_session_lost(proxy: ProxyStateWrapper) -> None:
            client = self.client_info[proxy.client_name]
            # if proxy.close_reason != CloseReason.PROXY_RECYCLE:
            client.proxy_pool.remove(proxy)

        def on_task_done(proxy: ProxyStateWrapper):
            client = self.client_info[proxy.client_name]
            if client.session:
                print(proxy._id)
                client.proxy_pool.put_nowait(proxy)

        proxy_server: Server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(
                    ProxyStateWrapper, self.client_info, on_proxy_session_made, on_proxy_session_lost, on_task_done
                ),
                host=server_settings.proxy_bind_host,
                port=0
            )
        )
        self.proxy_port = proxy_server.sockets[0].getsockname()[1]
        self.message_keeper.put(self.proxy_port)
        self.aexit.add_callback_when_cancel_all(proxy_server.close)

    def on_message_manager_session_made(self, client_name: str, epoch) -> None:
        client = self.client_info[client_name]
        client.open_session(epoch)

    def on_message_manager_session_lost(self, client_name: str) -> Future:
        client = self.client_info[client_name]
        return client.close_session()

