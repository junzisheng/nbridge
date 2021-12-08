from typing import Tuple, Any, Dict, Optional, List
from collections import defaultdict
from multiprocessing import Queue
from multiprocessing import Process
from multiprocessing.context import SpawnProcess
from multiprocessing.connection import Connection
from multiprocessing.reduction import recv_handle
from functools import partial
from asyncio import Task, Future, Transport, CancelledError
from concurrent.futures import ThreadPoolExecutor
import socket

import asyncio

from loguru import logger
from pydantic import BaseModel

from constants import CloseReason
from revoker import Revoker
from worker import ProcessWorker, Event
from state import State
from messager import Message, ProcessPipeMessageKeeper
from bridge_proxy.server import ProxyServer
from bridge_proxy.client import ProxyClient
from bridge_public.protocol import PublicProtocol
from settings import settings


class Statistic(BaseModel):
    idle: int = 0
    work: int = 0

    def total(self) -> int:
        return self.idle + self.work

    def __repr__(self):
        return f'idle:{self.idle} | work:{self.work}'

    __str__ = __repr__


class WorkerStruct(object):
    def __init__(self, socket_input: Connection, pid: int, port: int,
                 process: Process, message_keeper: ProcessPipeMessageKeeper) -> None:
        self.socket_input = socket_input
        self.pid = pid
        self.port = port
        self.process = process
        self.message_keeper = message_keeper
        self.proxy_state_shot: Dict[str, int] = defaultdict(lambda :0)

    def put(self, message: Message) -> None:
        self.message_keeper.send(message)


class ManagerWorker(ProcessWorker):
    def __init__(self, input: Connection, output: Connection, socket_recv_conn: Connection):
        super().__init__(input, output)
        self.token_map: Dict[str, str] = {}
        self.proxy_map: Dict[str, List[ProxyServer]] = defaultdict(list)
        self.socket_recv_conn = socket_recv_conn

    def start(self) -> None:
        def factory():
            return ProxyServer(lambda h: self.token_map.get(h),  self.report_proxy_state)

        server = self._loop.run_until_complete(
            self._loop.create_server(
                factory,
                host=settings.proxy_bind_host,
                port=0
            )
        )
        self.message_keeper.get_input().send(server.sockets[0].getsockname()[1])
        self._loop.create_task(
            self.listen_receive_socket()
        )

    async def listen_receive_socket(self) -> None:
        _executor = ThreadPoolExecutor(max_workers=1)

        def _receive_socket() -> tuple:
            fd: int = recv_handle(self.socket_recv_conn)
            host_name, end_point = self.socket_recv_conn.recv()
            return socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM), host_name, end_point

        while True:
            try:
                sock, host_name, end_point = await self._loop.run_in_executor(_executor, _receive_socket)
                proxy = self.require_proxy(host_name)
                if proxy:
                    proxy.state.to(State.WORK_PREPARE)
                    self._loop.create_task(self._loop.create_connection(
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
            except Exception as e:
                print(e.__class__)

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
            logger.info(f'Proxy 客户端【{protocol.host_name}】连接')
        elif pre_state != State.DISCONNECT and state == State.DISCONNECT:
            try:
                self.proxy_map[host_name].remove(protocol)
                logger.info(f'Proxy 客户端【{protocol.host_name}】断开连接')
            except ValueError:
                pass

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
    def __init__(self, input: Connection, output: Connection) -> None:
        super(ClientWorker, self).__init__(input, output)
        self.tasks: List[Task] = []
        self.protocols: List[ProxyClient] = []
        self.manager_connected = False

    def create_connection_task(self, factory: callable, host: str, port: int, d: int=0) -> None:
        async def delay() -> None:
            if d > 0:
                await asyncio.sleep(d)
            return await self._loop.create_connection(factory, host=host, port=port)
        task = self._loop.create_task(delay())
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
            pass

    def proxy_disconnect_handle(self, factory: callable, host: str, port: int, f: Future) -> None:
        if not self.manager_connected:
            return
        proxy_protocol: ProxyClient = f.result()
        if proxy_protocol.close_reason in [CloseReason.PING_TIMEOUT, CloseReason.UN_EXPECTED]:
            self.create_connection_task(factory, host=host, port=port)
        else:
            print('reason', proxy_protocol.close_reason)
            pass

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




