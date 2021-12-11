from typing import Tuple, Dict, Optional, List, Type, Union
import os
import sys
from itertools import chain
from collections import defaultdict
from multiprocessing import Process
from multiprocessing.connection import Connection
from multiprocessing.reduction import recv_handle, recvfds
from multiprocessing import Queue as process_Queue
from functools import partial
import asyncio
from asyncio import Future, Transport, CancelledError
from asyncio.base_events import Server
import socket

from loguru import logger

from constants import CloseReason
from revoker import Revoker
from worker import ProcessWorker, Event
from aexit_context import AexitContext
from state import State
from utils import safe_remove
from messager import Message, ProcessQueueMessageKeeper, ProcessPipeMessageKeeper, MessageKeeper
from proxy.server import ProxyServer
from proxy.client import ProxyClient
from public.protocol import PublicProtocol
from config.settings import server_settings, client_settings


class WorkerStruct(object):
    def __init__(
        self, socket_input: Connection, pid: int, port: int, process: Process,
        message_keeper: Union[ProcessPipeMessageKeeper, ProcessQueueMessageKeeper]
    ) -> None:
        self.socket_input = socket_input
        self.pid = pid
        self.port = port
        self.process = process
        self.message_keeper = message_keeper
        self.proxy_idle_map: Dict[str, int] = defaultdict(lambda: 0)

    def put(self, message: Message) -> None:
        self.message_keeper.send(message)


class ManagerWorker(ProcessWorker):
    def __init__(
        self, keeper_cls: Type[MessageKeeper], input_channel: Union[Connection, process_Queue],
        output_channel: Union[Connection, process_Queue], public_sock_channel: Connection
    ) -> None:
        super().__init__(keeper_cls, input_channel, output_channel)
        self.token_map: Dict[str, str] = {}
        self.proxy_map: Dict[str, List[ProxyServer]] = defaultdict(list)
        self.public_sock_channel = public_sock_channel
        self.proxy_server: Optional[Server] = None
        self.running_aexit: Dict[str, AexitContext] = defaultdict(AexitContext)

    def start(self) -> None:
        def _get_token(client_name: str) -> Optional[str]:
            return self.token_map.get(client_name)
        
        self.proxy_server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(ProxyServer, _get_token, self._on_proxy_state_change),
                host=server_settings.proxy_bind_host,
                port=0
            )
        )
        self.message_keeper.put(self.proxy_server.sockets[0].getsockname()[1])

        f = self.aexit.create_future()
        # 接收public socket
        self.listen_receive_public_sock()

        @f.add_done_callback
        def on_exit(_):
            self._loop.remove_reader(self.public_sock_channel.fileno())
            self.public_sock_channel.close()

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

    def listen_receive_public_sock(self) -> None:
        if sys.platform == 'win32':
            self._loop.add_reader(self.public_sock_channel.fileno(), self._on_new_public_sock)
        else:
            def _on_new_public_sock():
                self._on_new_public_sock(recvfds(listener_sock, 1)[0])
            listener_sock = socket.fromfd(self.public_sock_channel.fileno(), socket.AF_UNIX, socket.SOCK_STREAM)
            self._loop.add_reader(listener_sock.fileno(), _on_new_public_sock)

    def _send_message_to_change_manager_state_shot(self, client_name: str, idle: bool) -> None:
        """通知父进程proxy状态"""
        self.message_keeper.send(
            Message(
                Event.PROXY_STATE_CHANGE,
                self.pid,
                client_name,
                idle
            )
        )

    def _on_new_public_sock(self, fd: int) -> None:
        _sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        # socket.fromfd 复制了fd 这里关闭
        os.close(fd)
        public_port = _sock.getsockname()[1]
        public_config = server_settings.public_port_map[public_port]
        client_name = public_config.client.name
        proxy = self.require_proxy(client_name)
        if not proxy:
            _sock.close()
            self._send_message_to_change_manager_state_shot(client_name, True)
            logger.info('Unexpected no proxy!!')
        else:
            proxy.state.to(State.WORK_PREPARE)
            task = self.running_aexit[client_name].create_task(self._loop.create_connection(
                partial(
                    PublicProtocol,
                    (public_config.local_host, public_config.local_port),
                    proxy
                ),
                sock=_sock
            ))

            @task.add_done_callback
            def if_fail(f):
                try:
                    f.result()
                except Exception as e:
                    logger.exception(e)
                    if proxy.state.st == State.WORK_PREPARE:
                        proxy.state.to(State.IDLE)

    def _on_proxy_state_change(self, pre_state: str, state: str, protocol: ProxyServer) -> None:
        """处理proxy状态变更"""
        client_name = protocol.client_name
        if pre_state == State.WAIT_AUTH and state == State.IDLE:
            self.proxy_map[client_name].append(protocol)
            logger.info(f'Proxy Client【{protocol.client_name}】Connect')
        elif pre_state != State.DISCONNECT and state == State.DISCONNECT:
            if safe_remove(self.proxy_map[client_name], protocol):
                logger.info(f'Proxy Client【{protocol.client_name}】DisConnected')

        if pre_state != State.IDLE and state == State.IDLE:
            self._send_message_to_change_manager_state_shot(protocol.client_name, True)
        elif pre_state == State.IDLE and state == State.DISCONNECT:
            self._send_message_to_change_manager_state_shot(protocol.client_name, False)

    def current_proxy_state_map(self, client_name: str) -> Dict[str, int]:
        state_map: Dict[str, int] = defaultdict(lambda: 0)
        for p in self.proxy_map[client_name]:
            state_map[p.state.st] += 1
        return dict(state_map)

    def require_proxy(self, client_name: str) -> Optional[ProxyServer]:
        proxy_server_list = self.proxy_map[client_name]
        for proxy in proxy_server_list:
            if proxy.state.st == State.IDLE:
                return proxy
        return None

    def on_message_client_connect(self, token: str, client_name: str) -> None:
        self.token_map[client_name] = token

    def on_message_client_disconnect(self, client_name: str) -> None:
        if client_name in self.token_map:
            del self.token_map[client_name]
            for protocol in self.proxy_map[client_name]:
                protocol.rpc_call(
                    Revoker.call_set_close_reason,
                    CloseReason.CLIENT_DISCONNECT
                )
                protocol.transport.close()
            del self.proxy_map[client_name]
        self.running_aexit[client_name].cancel_all()

    def on_message_query_proxy_state(self) -> Tuple[Dict, int]:
        host_map = {}
        for client_name in self.proxy_map.keys():
            host_map[client_name] = self.current_proxy_state_map(client_name)
        return host_map, self.pid


class ClientWorker(ProcessWorker):
    def __init__(
        self, keeper_cls: Type[MessageKeeper], input_channel: Union[Connection, process_Queue],
        output_channel: Union[Connection, process_Queue]
    ) -> None:
        super(ClientWorker, self).__init__(keeper_cls, input_channel, output_channel)
        self.protocols: List[ProxyClient] = []
        self.manager_connected = False
        self.running_exit = AexitContext()

    async def _handle_stop(self) -> None:
        self.manager_connected = True
        for p in self.protocols:
            p.transport.close()

    def _on_proxy_session_made(self, protocol: ProxyClient) -> None:
        if self.manager_connected:
            self.protocols.append(protocol)
        else:
            protocol.transport.close()

    def _on_proxy_session_lost(self, token: str, port: int, protocol: ProxyClient) -> None:
        safe_remove(self.protocols, protocol)
        if protocol.close_reason in [CloseReason.PING_TIMEOUT, CloseReason.UN_EXPECTED]:
            self.create_connection_task(
                partial(
                    ProxyClient,
                    on_proxy_session_made=self._on_proxy_session_made,
                    on_proxy_session_lost=partial(self._on_proxy_session_lost, token, port),
                    token=token
                ),
                host=client_settings.server_host,
                port=port,
            )
        else:
            logger.info(f'Proxy Client Disconnect: {protocol.close_reason}')

    def create_connection_task(self, factory: callable, host: str, port: int, delay: int = 0) -> None:
        async def delay_create() -> None:
            if delay > 0:
                await asyncio.sleep(delay)
            return await self._loop.create_connection(factory, host=host, port=port)
        task = self.running_exit.create_task(delay_create())

        @task.add_done_callback
        def callback(f: Future) -> None:
            try:
                f.result()  # type: Transport, ProxyClient
            except CancelledError:
                pass
            except Exception as e:
                if self.manager_connected:
                    logger.info(f'【{host}】ProxyServer connect fail;retry')
                    self.create_connection_task(factory, host, port, 1)

    def on_message_proxy_create(self, port, token, size) -> None:
        self.manager_connected = True
        for i in range(size):
            self.create_connection_task(
                partial(
                    ProxyClient,
                    on_proxy_session_made=self._on_proxy_session_made,
                    on_proxy_session_lost=partial(self._on_proxy_session_lost, token, port),
                    token=token
                ),
                host=client_settings.server_host,
                port=port,
            )

    def on_message_manager_disconnect(self):
        self.manager_connected = False
        self.running_exit.cancel_all()

        for p in self.protocols:
            p.set_close_reason(CloseReason.SERVER_CLOSE)
            p.transport.close()
        self.protocols = []


