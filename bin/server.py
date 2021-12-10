from typing import Dict, List, Optional
from collections import defaultdict
import signal
from types import FrameType
import threading
import os
import asyncio
from asyncio import Future, futures
from asyncio.base_events import Server as Aserver
from multiprocessing import Pipe, Process, Queue as process_queue
from multiprocessing.reduction import recv_handle, send_handle
from functools import partial, cached_property

import socket
from loguru import logger
import uvloop

from state import State
from common_bases import Closer
from constants import CloseReason, HANDLED_SIGNALS
from worker import Event, run_worker
from revoker import Revoker
from registry import Registry
from aexit_context import AexitContext
from messager import ProcessPipeMessageKeeper, ProcessQueueMessageKeeper, Message, gather_message, broadcast_message
from bridge_manager.server import ManagerServer
from bridge_manager.manager_worker import WorkerStruct, ManagerWorker
from bridge_manager.monitor import MonitorServer
from bridge_public.protocol import start_public_server
from utils import safe_remove
from config.settings import server_settings

uvloop.install()


class Server(object):
    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self.closer = Closer(self._handle_stop)
        self.workers: Dict[int, WorkerStruct] = {}
        self.managers: Dict[str, ManagerServer] = {}
        self.aexit = AexitContext()
        self.manager_server: Optional[Aserver] = None
        self.public_servers: List[socket.socket] = []
        self.manager_registry = Registry()

    def run(self) -> None:
        self.install_signal_handlers()
        self.closer.run()
        self.run_worker()
        self.run_manager()
        self.run_monitor()
        self.run_public_server()
        self._loop.run_forever()
        self._loop.close()
        logger.info('Closed!')

    @cached_property
    def message_keepers(self) -> List[ProcessPipeMessageKeeper]:
        return [w.message_keeper for w in self.workers.values()]

    @cached_property
    def proxy_ports(self):
        return [w.port for w in self.workers.values()]

    def _on_manager_session_made(
            self, protocol: ManagerServer, token: str, process_notify_waiter: Future
    ) -> None:
        host_name = protocol.host_name

        gather = gather_message(
            self.message_keepers,
            Event.CLIENT_CONNECT,
            token=token,
            host_name=host_name
        )
        futures._chain_future(gather, process_notify_waiter)

    def _on_manager_session_lost(self, protocol: ManagerServer) -> None:
        host_name = protocol.host_name
        broadcast_message(self.message_keepers, Event.CLIENT_DISCONNECT, host_name=host_name)

    def _receive_public_socket(self, host_name: str, end_point: tuple, sock: socket.socket) -> None:
        def get_sort_list():
            struct_list = list(self.workers.values())
            sort_list = []
            for stru in struct_list:
                sort_list.append(
                    [stru.proxy_state_shot[host_name],
                     sum(
                         [x for x in stru.proxy_state_shot.values()]
                     ), stru.pid]
                )
            sort_list.sort(key=lambda x: [x[0], x[1]], reverse=True)
            return sort_list
        sort_list = get_sort_list()
        receiver = sort_list[0] if sort_list else None
        if receiver and receiver[0] > 0:
            worker = self.workers[receiver[2]]
            worker.proxy_state_shot[host_name] -= 1
            try:
                send_handle(worker.socket_input, sock.fileno(), worker.pid)
            except Exception as e:
                print(e)
            worker.socket_input.send((host_name, end_point))
        sock.close()

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
                    self._receive_public_socket,
                    public.client.name,
                    (public.local_host, public.local_port)
                ),
            )
            bind_port = s.getsockname()[1]
            logger.info(f'Public Server -【{bind_port}】 -> 【{name}】({public.local_host}:{public.local_port}) : Started')
            self.public_servers.append(s)

    def run_monitor(self):
        self.aexit.create_task(
            self.loop_monitor()
        )
        # MonitorServer.server = self
        # self._loop.run_until_complete(
        #     self._loop.create_server(
        #         MonitorServer,
        #         host=settings.monitor_bind_host,
        #         port=settings.monitor_bind_port
        #     )
        # )

    async def loop_monitor(self):
        while True:
            from prettytable import PrettyTable
            await asyncio.sleep(1)
            Column = ["PID", State.IDLE, State.WORK, State.WAIT_CLIENT_READY, State.WAIT_AUTH, State.DISCONNECT]
            table = PrettyTable(Column)
            table_dict = defaultdict(dict)
            pstr = ["="*50]
            for host_map, pid in await gather_message(
                self.message_keepers,
                Event.QUERY_PROXY_STATE
            ):
                for host, state in host_map.items():
                    default = defaultdict(lambda: 0)
                    default.update(state)
                    table_dict[host][pid] = default
            host_items = list(table_dict.items())
            host_items.sort(key=lambda x: x[0])
            for host, p_state in host_items:
                pstr.append('-'*25 + host + '-'*25)
                p_items = list(p_state.items())
                p_items.sort(key=lambda x:x[0])
                for pid, state in p_items:
                    row = [pid]
                    for col in Column[1:]:
                        row.append(state[col])
                    table.add_row(row)
            pstr.append(str(table))
            print("\n".join(pstr))

    def run_worker(self) -> None:
        for idx in range(server_settings.workers):
            # socket 文件描述符传递通道
            socket_output, socket_input = Pipe()
            # 进程通信通道
            input_queue, output_queue = process_queue(), process_queue()
            message_keeper = ProcessQueueMessageKeeper(self, input_queue, output_queue)
            pro = Process(
                target=run_worker,
                args=(ManagerWorker, ProcessQueueMessageKeeper, output_queue, input_queue, socket_output)
            )
            pro.daemon = True
            pro.start()
            socket_output.close()
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

    def on_message_proxy_state_change(self, pid: int, host_name: str, is_idle: bool) -> None:
        self.workers[pid].proxy_state_shot[host_name] += 1 if is_idle else -1

    def install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self.handle_exit)

    def handle_exit(self, sig: signal.Signals, frame: FrameType) -> None:
        self.closer.call_close()

    async def _handle_stop(self) -> None:
        waiter_list = []
        self.aexit.cancel_all()
        # 关闭manager server
        if self.manager_server:
            self.manager_server.close()
        waiter_list.append(self.manager_server.wait_closed())
        # 关闭public
        for s in self.public_servers:
            self._loop.remove_reader(s.fileno())
            s.close()
        logger.info('Public Servers Closed')
        # 关闭所有子进程
        workers = self.workers.values()
        for worker in workers:
            worker.put(
                Message(
                    Event.SERVER_CLOSE
                )
            )
            worker.socket_input.close()
        for worker in workers:
            worker.process.join()
            worker.message_keeper.stop()
            logger.info(f'Process 【{worker.pid}】Closed')
        # 关闭manager client
        for m in self.manager_registry.all_registry().values():
            m.rpc_call(
                Revoker.call_set_close_reason,
                CloseReason.SERVER_CLOSE,
            )
            m.transport.close()
            logger.info(f'Manager Client 【{m.host_name}】Closed')
        # 异步的都放到最后处理
        if waiter_list:
            await asyncio.gather(*waiter_list)
        logger.info('Manager Server Closed')

        self._loop.stop()


if __name__ == '__main__':
    server = Server()
    server.run()
