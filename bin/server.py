from typing import Dict, List, Optional
from collections import defaultdict
import os
import asyncio
from asyncio import Future, futures
from asyncio.base_events import Server as Aserver
from multiprocessing import Pipe, Process
from multiprocessing.reduction import recv_handle, send_handle
from functools import partial, cached_property

import socket
from loguru import logger

from common_bases import Bin
from constants import CloseReason
from worker import Event, run_worker
from revoker import Revoker
from registry import Registry
from messager import ProcessPipeMessageKeeper, Message, gather_message, broadcast_message
from manager.server import ManagerServer
from manager.monitor import MonitorServer
from manager.manager_worker import WorkerStruct, ManagerWorker
from public.protocol import start_public_server
from config.settings import server_settings


class Server(Bin):
    def __init__(self) -> None:
        super(Server, self).__init__()
        self.workers: Dict[int, WorkerStruct] = {}
        self.managers: Dict[str, ManagerServer] = {}
        self.manager_server: Optional[Aserver] = None
        self.public_servers: List[socket.socket] = []
        self.monitor_server: Optional[Aserver] = None
        self.manager_registry = Registry()

    def start(self) -> None:
        self.run_worker()
        self.run_manager()
        self.run_monitor()
        self.run_public_server()

    def run_manager(self) -> None:
        self.manager_server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(
                    ManagerServer,
                    manager_registry=self.manager_registry,
                    on_session_made=self._on_manager_session_made,
                    on_session_lost=self._on_manager_session_lost,
                    proxy_ports=self.proxy_ports,
                ),
                host=server_settings.manager_bind_host,
                port=server_settings.manager_bind_port,
            )
        )
        logger.info(f'Manager Server - [{server_settings.manager_bind_host}:{server_settings.manager_bind_port}] - '
                    f'{os.getpid()}: Started')

    def run_public_server(self) -> None:
        for name, public in server_settings.public_map.items():
            s = start_public_server(
                ('0.0.0.0', public.bind_port),
                partial(
                    self._on_public_new_socket,
                    public.client.name,
                ),
            )
            bind_port = s.getsockname()[1]
            logger.info(f'Public Server -【{bind_port}】 -> 【{name}】({public.local_host}:{public.local_port}) : Started')
            self.public_servers.append(s)

    def run_monitor(self):
        self.monitor_server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(MonitorServer, self._on_monitor_session_made),
                host=server_settings.manager_bind_host,
                port=server_settings.monitor_bind_port
            )
        )

    def run_worker(self) -> None:
        for idx in range(server_settings.workers):
            # socket 文件描述符传递通道
            socket_output, socket_input = Pipe()
            # 进程通信通道
            parent_input_channel, parent_output_channel = Pipe()
            child_input_channel, child_output_channel = Pipe()
            message_keeper = ProcessPipeMessageKeeper(self, parent_input_channel, child_output_channel)
            pro = Process(
                target=run_worker,
                args=(
                    ManagerWorker, ProcessPipeMessageKeeper,
                    child_input_channel, parent_output_channel,
                    socket_output
                )
            )
            pro.daemon = True
            pro.start()
            socket_output.close()
            child_input_channel.close()
            parent_output_channel.close()
            # 进程随机port启动proxy服务，这里获取port
            port = message_keeper.get()

            message_keeper.listen()
            self.workers[pro.pid] = WorkerStruct(
                socket_input=socket_input,
                pid=pro.pid,
                process=pro,
                message_keeper=message_keeper,
                port=port
            )
            logger.info(f'Proxy Server - 【{server_settings.proxy_bind_host}:{port}】 - {pro.pid}: Started')

    @cached_property
    def all_worker(self) -> List[WorkerStruct]:
        return list(self.workers.values())

    @cached_property
    def message_keepers(self) -> List[ProcessPipeMessageKeeper]:
        return [w.message_keeper for w in self.workers.values()]

    @cached_property
    def proxy_ports(self):
        return [w.port for w in self.workers.values()]

    def get_client_state_map(self) -> dict:
        host_proxy_state_map = defaultdict(dict)
        for pid, w in self.workers.items():
            for client_name, idle_count in w.proxy_idle_map.items():
                host_proxy_state_map[client_name][pid] = idle_count
        return host_proxy_state_map

    def _on_monitor_session_made(self, monitor: MonitorServer) -> None:
        monitor.notify_state(self.get_client_state_map())

    def _on_manager_session_made(
            self, protocol: ManagerServer, token: str, process_notify_waiter: Future
    ) -> None:
        client_name = protocol.client_name
        self.manager_registry.register(client_name, protocol)
        gather = gather_message(
            self.message_keepers,
            Event.CLIENT_CONNECT,
            token=token,
            client_name=client_name
        )
        futures._chain_future(gather, process_notify_waiter)

    def _on_manager_session_lost(self, protocol: ManagerServer) -> None:
        client_name = protocol.client_name
        broadcast_message(self.message_keepers, Event.CLIENT_DISCONNECT, client_name=client_name)
        gather = gather_message(
            self.message_keepers,
            Event.CLIENT_DISCONNECT,
            client_name=client_name
        )

        @gather.add_done_callback
        def on_clean_done(_):
            self.manager_registry.unregister(client_name)

    def _on_public_new_socket(self, client_name: str, sock: socket.socket) -> None:
        worker = self.choose_balance_worker(client_name)
        if worker and worker.proxy_idle_map[client_name] > 0:
            worker.proxy_idle_map[client_name] -= 1
            try:
                send_handle(worker.socket_input, sock.fileno(), worker.pid)
            except Exception as e:
                logger.exception(e)
        sock.close()

    def choose_balance_worker(self, client_name: str):
        """选择一个负载最低的worker"""
        workers = self.all_worker
        workers.sort(
            key=lambda w:  [w.proxy_idle_map[client_name], sum(list(w.proxy_idle_map.values()))],
            reverse=True
        )
        return workers[0]

    def on_message_proxy_state_change(self, pid: int, client_name: str, is_idle: bool) -> None:
        self.workers[pid].proxy_idle_map[client_name] += 1 if is_idle else -1
        MonitorServer.notify_all_state(self.get_client_state_map())

    async def do_handle_stop(self) -> None:
        waiter_list = []
        # 关闭manager server
        if self.manager_server:
            self.manager_server.close()
            waiter_list.append(self.manager_server.wait_closed())
        if self.monitor_server:
            self.monitor_server.close()
            waiter_list.append(self.monitor_server.wait_closed())
        # 关闭public
        for s in self.public_servers:
            self._loop.remove_reader(s.fileno())
            s.close()
        logger.info('Public Servers Closed')
        # 关闭所有子进程
        workers = self.workers.values()
        for worker in workers:
            worker.put(Message(Event.SERVER_CLOSE))
            worker.message_keeper.stop()
        for worker in workers:
            worker.process.join()
            worker.socket_input.close()
            worker.message_keeper.close()
            logger.info(f'Process【{worker.pid}】Closed')
        # 关闭manager client
        for m in self.manager_registry.all_registry().values():
            m.rpc_call(
                Revoker.call_set_close_reason,
                CloseReason.SERVER_CLOSE,
            )
            m.transport.close()
            logger.info(f'Manager Client 【{m.client_name}】Closed')
        # 异步的都放到最后处理
        if waiter_list:
            await asyncio.gather(*waiter_list)
        logger.info('Manager Server Closed')


if __name__ == '__main__':
    server = Server()
    server.run()
