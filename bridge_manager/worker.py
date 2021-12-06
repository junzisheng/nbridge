from typing import Tuple, Any, Dict, Optional, List
from collections import defaultdict
from multiprocessing import Queue
from multiprocessing.context import SpawnProcess
from functools import partial
from asyncio import Task, Future, Transport, CancelledError
import socket

import asyncio

import os
from loguru import logger
from pydantic import BaseModel

from constants import CloseReason
from bridge_proxy.server import ProxyServer
from bridge_proxy.client import ProxyClient
from revoker import Revoker
from bridge_public.protocol import PublicProtocol
from settings import settings
from worker import ProcessWorker, create_event, Event
from state import State

g = 0


class Statistic(BaseModel):
    idle: int = 0
    work: int = 0

    def total(self) -> int:
        return self.idle + self.work

    def __repr__(self):
        return f'idle:{self.idle} | work:{self.work}'

    __str__ = __repr__


class WorkerStruct(object):
    def __init__(self, pid: int, port: int, process: SpawnProcess, input_queue: Queue, output_queue: Queue) -> None:
        self.pid = pid
        self.port = port
        self.process = process
        self.input_queue = input_queue
        self.out_queue = output_queue
        self.proxy_statistic: Dict[str, Statistic] = defaultdict(Statistic)

    def get(self) -> Any:
        return self.out_queue.get()

    def put(self, data: Any) -> None:
        self.input_queue.put(data)


class ManagerWorker(ProcessWorker):
    def __init__(self, output_queue: Queue, input_queue: Queue):
        super().__init__(output_queue, input_queue)
        self.token_map: Dict[str, str] = {}
        self.proxy_map: Dict[str, List[ProxyServer]] = defaultdict(list)

    def start(self) -> None:
        def factory():
            return ProxyServer(lambda h: self.token_map.get(h), self.report_state)

        server = self._loop.run_until_complete(
            self._loop.create_server(
                factory,
                host=settings.proxy_bind_host,
                port=0
            )
        )
        self.input_queue.put(server.sockets[0].getsockname()[1])

    def report_state(self, pre_state: str, state: str, protocol: ProxyServer) -> None:
        if pre_state == State.WAIT_AUTH and state == State.IDLE:
            self.proxy_map[protocol.host_name].append(protocol)
            logger.info(f'Proxy 客户端【{protocol.host_name}】连接')
        elif pre_state != State.DISCONNECT and state == State.DISCONNECT:
            try:
                self.proxy_map[protocol.host_name].remove(protocol)
                logger.info(f'Proxy 客户端【{protocol.host_name}】断开连接')
            except ValueError:
                pass
        idle = work = 0
        state_map = defaultdict(lambda : 0)
        for p in self.proxy_map[protocol.host_name]:
            state_map[p.state.st] += 1
            if p.state.st == State.IDLE:
                idle += 1
            elif p.state.st not in [State.WAIT_AUTH, State.IDLE, State.DISCONNECT]:
                work += 1
        # print(state_map)
        self.input_queue.put(
            create_event(
                Event.PROXY_STATE_CHANGE,
                protocol.host_name,
                idle,
                work,
                dict(state_map)
            )
        )

    def on_client_connect(self, token: str, host_name: str) -> None:
        self.token_map[host_name] = token

    def on_client_disconnect(self, host_name: str) -> None:
        if host_name in self.token_map:
            del self.token_map[host_name]
            for protocol in self.proxy_map[host_name]:
                protocol.rpc_call(
                    Revoker.call_set_close_reason,
                    CloseReason.CLIENT_DISCONNECT
                )
                protocol.transport.close()
            del self.proxy_map[host_name]

    def on_public_create(self, host_name: str, sock: socket.socket, end_point: tuple) -> None:
        self._loop.create_task(self._loop.create_connection(
            partial(PublicProtocol, end_point, partial(self.require_proxy, host_name)),
            sock=sock
        ))

    def require_proxy(self, host_name: str) -> Optional[ProxyServer]:
        proxy_server_list = self.proxy_map[host_name]
        for proxy in proxy_server_list:
            if proxy.state.st == State.IDLE:
                return proxy
        return None


class ClientWorker(ProcessWorker):
    def __init__(self, output_queue: Queue, input_queue: Queue) -> None:
        super(ClientWorker, self).__init__(output_queue, input_queue)
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
                    self.create_connection_task(factory, host, port, 0)

    def proxy_connect_handle(self, p: ProxyClient, _) -> None:
        global g
        g += 1
        print(g, os.getpid())
        if self.manager_connected:
            self.protocols.append(p)
        else:
            print(self.manager_connected)

    def proxy_disconnect_handle(self, factory: callable, host: str, port: int, f: Future) -> None:
        if not self.manager_connected:
            return
        proxy_protocol: ProxyClient = f.result()
        if proxy_protocol.close_reason in [CloseReason.PING_TIMEOUT, CloseReason.UN_EXPECTED]:
            self.create_connection_task(factory, host=host, port=port)
        else:
            print('reason', proxy_protocol.close_reason)
            pass

    def on_proxy_create(self, host_name, port, token, size) -> None:
        self.manager_connected = True
        for i in range(size):
            self.create_connection_task(
                partial(ProxyClient, host_name, token),
                host=settings.manager_remote_host,
                port=port,
            )

    def on_manager_disconnect(self):
        self.manager_connected = False
        for task in self.tasks:
            if not task.done():
                task.cancel()
        self.tasks = []

        for p in self.protocols:
            p.set_close_reason(CloseReason.CLIENT_DISCONNECT)
            p.transport.close()
        self.protocols = []




