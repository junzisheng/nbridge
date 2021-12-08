from typing import Tuple, Dict, Optional, List, Type, Union
from itertools import chain
from collections import defaultdict
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.reduction import recv_handle
from multiprocessing import Queue as process_Queue
from functools import partial
import asyncio
from asyncio import Task, Future, Transport, CancelledError
from asyncio.base_events import Server
import socket

from loguru import logger

from constants import CloseReason
from revoker import Revoker
from worker import ProcessWorker, Event
from state import State
from utils import safe_remove
from messager import Message, ProcessQueueMessageKeeper, ProcessPipeMessageKeeper, MessageKeeper
from bridge_proxy.server import ProxyServer
from bridge_proxy.client import ProxyClient
from bridge_public.protocol import PublicProtocol
from settings import settings


class WorkerStruct(object):
    def __init__(self, socket_input: Connection, pid: int, port: int,
                 process: Process,
                 message_keeper: Union[ProcessPipeMessageKeeper, ProcessQueueMessageKeeper]) -> None:
        self.socket_input = socket_input
        self.pid = pid
        self.port = port
        self.process = process
        self.message_keeper = message_keeper
        self.proxy_state_shot: Dict[str, int] = defaultdict(lambda :0)

    def put(self, message: Message) -> None:
        self.message_keeper.send(message)


class ManagerWorker(ProcessWorker):
    def __init__(self, keeper_cls: Type[MessageKeeper], input_channel: Union[Connection, process_Queue],
                 output_channel: Union[Connection, process_Queue], socket_recv_conn: Connection) -> None:
        super().__init__(keeper_cls, input_channel, output_channel)
        self.token_map: Dict[str, str] = {}
        self.proxy_map: Dict[str, List[ProxyServer]] = defaultdict(list)
        self.socket_recv_conn = socket_recv_conn
        self.proxy_server: Optional[Server] = None

    def start(self) -> None:
        def factory():
            return ProxyServer(lambda h: self.token_map.get(h),  self.report_proxy_state)

        self.proxy_server = self._loop.run_until_complete(
            self._loop.create_server(
                factory,
                host=settings.proxy_bind_host,
                port=0
            )
        )
        self.message_keeper.put(self.proxy_server.sockets[0].getsockname()[1])
        self.aexit_context.create_task(
            self.listen_receive_socket()
        )

    async def _handle_stop(self) -> None:
        self.token_map = {}
        waiters = []
        # 关闭proxy server
        if self.proxy_server:
            self.proxy_server.close()
            waiters.append(self.proxy_server.wait_closed())
        # 断开所有proxy client
        for proxy in chain(*self.proxy_map.values()):  # type: ProxyServer
            proxy.rpc_call(
                Revoker.call_set_close_reason,
                CloseReason.SERVER_CLOSE,
            )
        logger.info(f'Worker Process【{self.pid}】: Proxy Client Closed')
        if waiters:
            await asyncio.gather(*waiters)
        logger.info(f'Worker Process【{self.pid}】: Proxy Server Closed')

    async def listen_receive_socket(self) -> None:
        """接收public请求并序列为socket"""
        def _receive_socket() -> tuple:
            fd: int = recv_handle(self.socket_recv_conn)
            _host_name, _end_point = self.socket_recv_conn.recv()
            return socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM), _host_name, _end_point

        while True:
            try:
                sock, host_name, end_point = await self._loop.run_in_executor(None, _receive_socket)
                proxy = self.require_proxy(host_name)
                if proxy:
                    proxy.state.to(State.WORK_PREPARE)
                    self.aexit_context.create_task(self._loop.create_connection(
                        partial(PublicProtocol, end_point, proxy),
                        sock=sock
                    ))
                else:
                    sock.close()
                    self.message_keeper.send(
                        Message(
                            Event.PROXY_STATE_CHANGE,
                            self.pid,
                            host_name,
                            False
                        )
                    )
            except EOFError:
                # 主进程退出，这里正好退出循环，否则子进程无法正常关闭
                break

    def current_proxy_state_map(self, host_name: str) -> Dict[str, int]:
        state_map: Dict[str, int] = defaultdict(lambda: 0)
        for p in self.proxy_map[host_name]:
            state_map[p.state.st] += 1
        return dict(state_map)

    def report_proxy_state(self, pre_state: str, state: str, protocol: ProxyServer) -> None:
        """上报proxy状态"""
        host_name = protocol.host_name
        if pre_state == State.WAIT_AUTH and state == State.IDLE:
            self.proxy_map[host_name].append(protocol)
            logger.info(f'Proxy Client【{protocol.host_name}】Connect')
        elif pre_state != State.DISCONNECT and state == State.DISCONNECT:
            if safe_remove(self.proxy_map[host_name], protocol):
                logger.info(f'Proxy Client【{protocol.host_name}】DisConnected')

        if pre_state != State.IDLE and state == State.IDLE:
            self.message_keeper.send(
                Message(
                    Event.PROXY_STATE_CHANGE,
                    self.pid,
                    host_name,
                    True
                )
            )

    def require_proxy(self, host_name: str) -> Optional[ProxyServer]:
        proxy_server_list = self.proxy_map[host_name]
        for proxy in proxy_server_list:
            if proxy.state.st == State.IDLE:
                return proxy
        return None

    def on_message_client_connect(self, token: str, host_name: str) -> None:
        self.token_map[host_name] = token

    def on_message_client_disconnect(self, host_name: str) -> None:
        if host_name in self.token_map:
            del self.token_map[host_name]
            for protocol in self.proxy_map[host_name]:
                protocol.rpc_call(
                    Revoker.call_set_close_reason,
                    CloseReason.CLIENT_DISCONNECT
                )
                protocol.transport.close()
            del self.proxy_map[host_name]

    def on_message_query_proxy_state(self) -> Tuple[Dict, int]:
        host_map = {}
        for host_name in self.proxy_map.keys():
            host_map[host_name] = self.current_proxy_state_map(host_name)
        return host_map, self.pid


class ClientWorker(ProcessWorker):
    def __init__(self, keeper_cls: Type[MessageKeeper], input_channel: Union[Connection, process_Queue],
                 output_channel: Union[Connection, process_Queue]) -> None:
        super(ClientWorker, self).__init__(keeper_cls, input_channel, output_channel)
        self.tasks: List[Task] = []
        self.protocols: List[ProxyClient] = []
        self.manager_connected = False

    def create_connection_task(self, factory: callable, host: str, port: int, delay: int = 0) -> None:
        async def delay_create() -> None:
            if delay > 0:
                await asyncio.sleep(d)
            return await self._loop.create_connection(factory, host=host, port=port)
        task = self._loop.create_task(delay_create())
        self.tasks.append(task)

        @task.add_done_callback
        def callback(f: Future) -> None:
            try:
                self.tasks.remove(task)
            except:
                pass
            try:
                _, p = f.result()  # type: Transport, ProxyClient
                p.auth_success_waiter.add_done_callback(partial(self.proxy_connect_handle, p))
                p.disconnect_waiter.add_done_callback(partial(self.proxy_disconnect_handle, factory, host, port))
            except CancelledError:
                pass
            except Exception as e:
                if self.manager_connected:
                    logger.info(f'【{host}】ProxyServer connect fail;retry')
                    self.create_connection_task(factory, host, port, 1)

    def proxy_connect_handle(self, p: ProxyClient, _) -> None:
        if self.manager_connected:
            self.protocols.append(p)
        else:
            p.transport.close()

    def proxy_disconnect_handle(self, factory: callable, host: str, port: int, f: Future) -> None:
        proxy_protocol: ProxyClient = f.result()
        if proxy_protocol.close_reason in [CloseReason.PING_TIMEOUT, CloseReason.UN_EXPECTED]:
            if self.manager_connected:
                self.create_connection_task(factory, host=host, port=port)
        else:
            logger.info(f'Proxy Client Disconnect: {proxy_protocol.close_reason}')
        safe_remove(self.protocols, proxy_protocol)

    def on_message_proxy_create(self, host_name, port, token, size) -> None:
        self.manager_connected = True
        for i in range(size):
            self.create_connection_task(
                partial(ProxyClient, host_name, token),
                host=settings.manager_remote_host,
                port=port,
            )

    def on_message_manager_disconnect(self):
        self.manager_connected = False
        for task in self.tasks:
            if not task.done():
                task.cancel()
        self.tasks = []

        for p in self.protocols:
            p.set_close_reason(CloseReason.CLIENT_DISCONNECT)
            p.transport.close()
        self.protocols = []




