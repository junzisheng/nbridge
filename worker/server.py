from typing import Tuple, Dict, Optional, List, Type, Union, Mapping
import os
import asyncio
import socket
from collections import defaultdict
from multiprocessing.connection import Connection
from functools import partial
from asyncio.base_events import Server
from asyncio import Task, CancelledError, TimeoutError, Future
from async_timeout import timeout

from loguru import logger
from tinydb import TinyDB, where
from tinydb.operations import set
from tinydb.storages import MemoryStorage

from constants import CloseReason, ManagerState
from worker.bases import ProcessWorker, ProxyStateWrapper, ClientStruct, ProxyPool
from aexit_context import AexitContext
from messager import Message, Event, ProcessPipeMessageKeeper, SocketChannel
from public.protocol import PublicProtocol, HttpPublicProtocol
from utils import safe_remove, catch_cor_exception, socket_fromfd, protocol_sockname, pop_table_row
from config.settings import server_settings

PubProType = Union[PublicProtocol, HttpPublicProtocol]


db = TinyDB(storage=MemoryStorage)
client_table = db.table('client_table')
public_table = db.table('public_table')
sock_table = db.table('sock_table')


@catch_cor_exception
async def _proxy_public_pair(
    proxy_pool: ProxyPool, public: PubProType, local_addr: Tuple[str, int], delay: int
) -> None:
    try:
        async with timeout(delay):
            proxy = await proxy_pool.get()  # type: ProxyStateWrapper
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
            client_name = proxy.client_name
            client = client_table.get(where('client_name') == client_name)
            if not client or client['state'] != ManagerState.session:
                proxy.close(CloseReason.MANAGE_CLIENT_LOST)
            else:
                client['proxy_pool'].put_nowait(proxy)

        def on_proxy_session_lost(proxy: ProxyStateWrapper) -> None:
            client_name = proxy.client_name
            client = client_table.get(where('client_name') == client_name)
            if client:
                client['proxy_pool'].remove(proxy)

        def on_task_done(proxy: ProxyStateWrapper):
            client = client_table.get(where('client_name') == proxy.client_name)
            if client and client['state'] == ManagerState.session:
                client['proxy_pool'].put_nowait(proxy)

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
        http_aexit_map: Dict[Union[str, int], AexitContext] = defaultdict(AexitContext)
        port_map: Dict[int, Server] = {}

        @self.aexit.add_callback_when_cancel_all
        def _():
            for s in port_map.values():
                s.close()

        def manager_session_made(client_name: str, epoch) -> None:

            def perform_update(doc: Dict) -> None:
                doc['state'] = ManagerState.session
                doc['epoch'] = epoch
            client_table.update(
                perform_update,
                where('client_name') == client_name
            )

        def manager_session_lost(client_name: str) -> Future:

            def perform_update(doc: Dict) -> None:
                doc['state'] = ManagerState.idle
                doc['epoch'] = -1
            client_table.update(
                perform_update,
                where('client_name') == client_name
            )
            # close public protocol; close proxy protocol; public pair;
            client = client_table.get(where('client_name') == client_name)
            aexit: AexitContext = client['aexit']
            aexit.cancel_all()
            proxy_pool: ProxyPool = client['proxy_pool']
            # proxy_pool close will close all public protocol or cancel pair task
            return proxy_pool.close_all(CloseReason.MANAGE_CLIENT_LOST)

        # =============== commander ===============
        def client_add(client_name: str, token: str) -> None:
            proxy_pool = ProxyPool(
                self._loop,
                partial(self.async_create_proxy, client_name),
                min_size=server_settings.proxy_pool_size
            )
            proxy_pool.start_recycle_task()
            aexit = AexitContext()
            client = {
                'client_name': client_name, 'token': token, 'proxy_pool': proxy_pool,
                'aexit': aexit, 'epoch': -1
            }
            client_table.insert(client)

        def client_remove(client_name: str) -> Future:
            client = pop_table_row(client_table, where('client_name') == client_name)
            aexit: AexitContext = client['aexit']
            proxy_pool: ProxyPool = client['proxy_pool']
            proxy_pool.recycle_task.cancel()
            aexit.cancel_all()
            f = proxy_pool.close_all(CloseReason.MANAGE_CLIENT_LOST)

            # @f.add_done_callback
            # def _(_):
            #     del self.client_info[client_name]
            #     del client_aexit_map[client_name]
            # client_aexit_map[client_name].cancel_all()
            # _close_public_server(client_name=client_name)
            return f

        # public proxy pair
        pair_tasks: Dict[PubProType, Task] = {}
        public_server: List[Tuple[Server, str, int]] = []
        public_protocol_list: List[PubProType] = []

        # close public server by port or client_name
        def _close_public_server(client_name: Optional[str] = None, port: Optional[int] = None) -> None:
            remove_list: List[Tuple[Server, str, int]] = []
            for ps in public_server:
                if client_name and ps[1] == client_name or port and ps[2] == port:
                    remove_list.append(ps)
            for ps in remove_list:
                public_server.remove(ps)
                ps[0].close()

        def _on_public_connection_lost(public: PublicProtocol) -> None:
            safe_remove(public_protocol_list, public)
            task = pair_tasks.get(public)
            if task:  # cancel pair task
                task.cancel()

        def tcp_add(client_name: str, local_addr: Tuple[str, int]) -> None:
            sock, port = receive_sock(self.socket_receiver)

            def on_connection_made(public: PublicProtocol) -> None:
                client = client_table.get(where('client_name') == client_name)
                if not client:  # client maybe lost or removed
                    public.transport.close()
                else:
                    # client aexit
                    aexit: AexitContext = client['aexit']
                    proxy_pool: ProxyPool = client['proxy_pool']
                    public_protocol_list.append(public)
                    _task = aexit.create_task(
                        _proxy_public_pair(proxy_pool, public, local_addr, server_settings.proxy_wait_timeout)
                    )
                    pair_tasks[public] = _task
                    task.add_done_callback(lambda _x: safe_remove(pair_tasks,  public))
                    # port sock aexit
                    sock_row = sock_table.get((where('port') == port) & (where('type') == 'tcp'))
                    _sock_aexit: AexitContext = sock_row['aexit']
                    _sock_aexit.monitor_future(_task)

            async def create_tcp_server():
                server: Server = await self._loop.create_server(
                    partial(PublicProtocol, on_connection_made, _on_public_connection_lost),
                    sock=sock
                )
                sock_table.update(
                    set('server', server),
                    where('port') == port
                )

            _client = client_table.get(where('client_name') == client_name)
            client_aexit: AexitContext = _client['aexit']
            sock_aexit: AexitContext = AexitContext()
            sock_table.insert(
                {'port': port, 'server': None, 'aexit': sock_aexit}
            )
            public_table.insert(
                {'client_name': client_name, 'type': 'tcp',
                 'local_host': local_addr[0], 'local_port': local_addr[1],
                 'custom_domain': '', 'port': port
                 }
            )
            task = client_aexit.create_task(create_tcp_server())
            sock_aexit.monitor_future(task)

        def tcp_remove(port: int) -> None:
            sock_row = pop_table_row(sock_table, (where('port') == port) & (where('type') == 'tcp'))
            aexit: AexitContext = sock_row['aexit']
            server: Optional[Server] = sock_row['server']
            if server:
                server.close()
            aexit.cancel_all()
            for public in public_protocol_list:
                if protocol_sockname(public.transport)[1] == port:
                    public.transport.close()

        http_map: Dict[str, Tuple[str, Tuple[str, int]]] = {}

        def http_add(
            _client_name: str, _local_addr: Tuple[str, int], _custom_domain: str, sock: bool
        ) -> None:
            http_map[_custom_domain] = (_client_name, _local_addr)
            if sock:
                sock, port = receive_sock(self.socket_receiver)

                def on_request_event_received(public: HttpPublicProtocol) -> None:
                    custom_domain = public.domain
                    client_name, local_addr = http_map.get(custom_domain, (None, [None, None]))
                    if client_name is None:
                        public.transport.close()
                        return
                    client = self.client_info.get(client_name)
                    if not client or not client.session:
                        public.transport.close()
                        return
                    public_protocol_list.append(public)
                    task = client_aexit_map[client_name].create_task(
                        _proxy_public_pair(client, public, local_addr, server_settings.proxy_wait_timeout)
                    )
                    pair_tasks[public] = task
                    task.add_done_callback(lambda _x: safe_remove(pair_tasks, public))
                    http_aexit_map[port].monitor_future(task)
                    http_aexit_map[custom_domain].monitor_future(task)

                async def create_http_server():
                    server: Server = await self._loop.create_server(
                        partial(HttpPublicProtocol, on_request_event_received, _on_public_connection_lost),
                        sock=sock
                    )
                    public_server.append((server, _client_name, sock.getsockname()[1]))
                http_aexit_map[port].create_task(create_http_server())

        return {
            Event.MANAGER_SESSION_MADE: manager_session_made,
            Event.MANAGER_SESSION_LOST: manager_session_lost,
            Event.CLIENT_ADD: client_add,
            Event.CLIENT_REMOVE: client_remove,
            Event.TCP_ADD: tcp_add,
            Event.TCP_REMOVE: tcp_remove,
            Event.HTTP_ADD: http_add
        }


def receive_sock(sock_receiver: SocketChannel) -> Tuple[socket.socket, int]:
    fileno = sock_receiver.receive()
    sock = socket_fromfd(fileno)
    os.close(fileno)  # close duplicate fd
    port = sock.getsockname()[1]
    return sock, port







