from typing import Dict, List, Optional
from collections import defaultdict
from itertools import chain
import os
from asyncio import Future
from asyncio.base_events import Server as Aserver
from multiprocessing import Pipe, Process
from functools import partial, cached_property

import socket
from loguru import logger

from common_bases import Bin
from constants import CloseReason
from messager import Event
from worker.bases import run_worker, ServerWorkerStruct
from worker.server import ManagerWorker
from registry import Registry
from messager import ProcessPipeMessageKeeper, Message, gather_message, broadcast_message
from manager.server import ManagerServer
# from manager.monitor import MonitorServer
from config.settings import server_settings


class Server(Bin):
    def __init__(self) -> None:
        super(Server, self).__init__()
        self.workers: Dict[int, ServerWorkerStruct] = {}
        self.managers: Dict[str, ManagerServer] = {}
        self.manager_server: Optional[Aserver] = None
        self.public_servers: Dict[str: List[socket.socket]] = defaultdict(list)
        self.monitor_server: Optional[Aserver] = None
        self.manager_registry = Registry()

    def start(self) -> None:
        self.run_manager()
        self.run_public_server()
        self.run_worker()
        # self.run_monitor()

    def run_manager(self) -> None:
        def on_manager_session_made(manager: ManagerServer) -> Future:
            client_name = manager.client_name
            self.manager_registry.register(client_name, manager)
            return gather_message(
                self.message_keepers, Event.MANAGER_SESSION_MADE, client_name=client_name,
                epoch=server_settings.client_map[client_name].epoch
            )

        def on_manager_session_lost(protocol: ManagerServer) -> None:
            client_name = protocol.client_name
            server_settings.client_map[client_name].increase_epoch()
            gather = gather_message(
                self.message_keepers,
                Event.MANAGER_SESSION_LOST,
                client_name=client_name,
            )
            self.aexit.monitor_future(gather)

            @gather.add_done_callback
            def _(_):
                self.manager_registry.unregister(client_name)

        self.manager_server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(
                    ManagerServer,
                    self.workers,
                    manager_registry=self.manager_registry,
                    on_session_made=on_manager_session_made,
                    on_session_lost=on_manager_session_lost,
                ),
                host=server_settings.manager_bind_host,
                port=server_settings.manager_bind_port,
            )
        )
        logger.info(f'Manager Server - [{server_settings.manager_bind_host}:{server_settings.manager_bind_port}] - '
                    f'{os.getpid()}: Started')

        @self.aexit.add_callback_when_cancel_all
        def _():
            for manager in self.manager_registry.all_registry().values():  # type: ManagerServer
                manager.close(CloseReason.SERVER_CLOSE)

            logger.info(f'Manager Server Closed Done')

    def run_public_server(self) -> None:
        for name, public in server_settings.public_map.items():
            client_name = public.client.name
            public_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            public_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
            public_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)
            public_server.setblocking(False)
            public_server.bind((server_settings.manager_bind_host, public.bind_port))
            public_server.listen(50)
            public_server.set_inheritable(True)
            self.public_servers[client_name].append(public_server)
            logger.info(f'Public Server - {server_settings.manager_bind_host}:{public.bind_port} Started')

        @self.aexit.add_callback_when_cancel_all
        def _():
            for ps in chain(*self.public_servers.values()):  # type: socket.socket
                ps.close()
            logger.info("Public Server Closed Done")

    def run_monitor(self):
        monitors: List[MonitorServer] = []

        def on_monitor_session_made(monitor):
            monitors.append(monitor)

        def on_monitor_session_lost(monitor):
            monitors.remove(monitor)

        self.monitor_server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(MonitorServer, on_monitor_session_made, on_monitor_session_lost),
                host=server_settings.manager_bind_host,
                port=server_settings.monitor_bind_port
            )
        )

        @self.aexit.add_callback_when_cancel_all
        def _():
            self.monitor_server.close()
            logger.info("Monitor Server Closed Done")
            for mon in monitors:
                mon.close(CloseReason.SERVER_CLOSE)

    def run_worker(self) -> None:
        for idx in range(server_settings.workers):
            # 进程通信通道
            parent_input_channel, parent_output_channel = Pipe()
            child_input_channel, child_output_channel = Pipe()
            message_keeper = ProcessPipeMessageKeeper(self, parent_input_channel, child_output_channel)
            pro = Process(
                target=run_worker,
                args=(
                    ManagerWorker, ProcessPipeMessageKeeper,
                    child_input_channel, parent_output_channel,
                    self.public_servers,
                )
            )
            pro.daemon = True
            pro.start()
            child_input_channel.close()
            parent_output_channel.close()
            # 进程随机port启动proxy服务，这里获取port
            proxy_port = message_keeper.get()

            message_keeper.listen()
            self.workers[pro.pid] = ServerWorkerStruct(
                process=pro,
                message_keeper=message_keeper,
                proxy_port=proxy_port
            )
            logger.info(f'Proxy Server - 【{server_settings.proxy_bind_host}:{proxy_port}】 - {pro.pid}: Started')

    @cached_property
    def all_worker(self) -> List[ServerWorkerStruct]:
        return list(self.workers.values())

    @cached_property
    def message_keepers(self) -> List[ProcessPipeMessageKeeper]:
        return [w.message_keeper for w in self.workers.values()]

    def on_message_proxy_create(self, client_name: str, port, num=1) -> None:
        manager = self.manager_registry.get(client_name)  # type: ManagerServer
        if manager and manager.session_status == 1:
            manager.create_proxy(port, num)

    async def do_handle_stop(self) -> None:
        workers = self.workers.values()
        for worker in workers:
            worker.put(Message(Event.SERVER_CLOSE))
            worker.message_keeper.stop()
        for worker in workers:
            worker.process.join()
            worker.message_keeper.close()
            logger.info(f'Process【{worker.pid}】Closed')
        # 关闭manager client
        logger.info('Manager Server Closed')


if __name__ == '__main__':
    server = Server()
    server.run()
