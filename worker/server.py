from typing import Tuple, Dict, Optional, List, Type, Union
from multiprocessing.connection import Connection
from functools import partial
from asyncio.base_events import Server
from asyncio import Task, CancelledError, TimeoutError, Future
import socket
from async_timeout import timeout

from loguru import logger

from constants import CloseReason
from worker.bases import ProcessWorker, ProxyStateWrapper, ClientStruct
from messager import Message, Event, ProcessPipeMessageKeeper
from public.protocol import PublicProtocol
from utils import safe_remove
from config.settings import server_settings


async def _proxy_public_pair(client: ClientStruct, public: PublicProtocol, delay: int) -> None:
    try:
        async with timeout(delay):
            proxy = await client.proxy_pool.get()  # type: ProxyStateWrapper
    except (CancelledError, TimeoutError):
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
        self.initialize()

    def initialize(self) -> None:
        client_info: Dict[str, ClientStruct] = {}
        for client_name, client in server_settings.client_map.items():
            client_info[client_name] = ClientStruct(
                name=client_name, token=client.token, public_sockets=self.public_servers[client_name]
            )
            self.aexit.mount(client_info[client_name].aexit)
        self.client_info = client_info

    def start(self) -> None:
        self.run_proxy_server()
        self.run_public()

        @self.aexit.add_callback_when_cancel_all
        def _():
            for client in self.client_info.values():
                client.close_session()

    async def _handle_stop(self) -> None:
        logger.info(f'Worker Process【{self.pid}】: Exit')

    def run_public(self) -> None:
        for client_name, sock_list in self.public_servers.items():
            client = self.client_info[client_name]
            # 配对任务
            pair_tasks: Dict[PublicProtocol, Task] = {}

            def on_connection_made(public: PublicProtocol) -> None:
                if not client.session:
                    public.transport.close()
                else:
                    task = client.aexit.create_task(
                        _proxy_public_pair(client, public, server_settings.proxy_wait_timeout)
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
                        partial(PublicProtocol, on_connection_made, on_connection_lost),
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
            client.proxy_pool.remove(proxy)

        def on_task_done(proxy: ProxyStateWrapper):
            print('--')
            client = self.client_info[proxy.client_name]
            if client.session:
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
        self.message_keeper.put(proxy_server.sockets[0].getsockname()[1])
        self.aexit.add_callback_when_cancel_all(proxy_server.close)

    def on_message_manager_session_made(self, client_name: str, epoch) -> None:
        client = self.client_info[client_name]
        client.open_session(epoch)

    def on_message_manager_session_lost(self, client_name: str) -> Future:
        client = self.client_info[client_name]
        return client.close_session()

