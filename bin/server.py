from typing import Dict, List
from collections import defaultdict
import sys
import signal
from types import FrameType
import threading
import os
import asyncio
from asyncio import Future, futures
from multiprocessing import Queue
from functools import partial

import socket
from loguru import logger
import uvloop
s = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, s)

from sub_process import get_subprocess
from state import State
from worker import Event
from messager import ProcessMessageKeeper, Message, WaiterMessage, gather_message
from bridge_manager.server import ManagerServer
from bridge_manager.worker import WorkerStruct
from bridge_manager.worker import ManagerWorker
from bridge_manager.monitor import MonitorServer
from bridge_public.protocol import start_public_server
from settings import settings


uvloop.install()

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)


class Server(object):
    exit_event = threading.Event()

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self.workers: Dict[int, WorkerStruct] = {}
        self.managers: Dict[str, ManagerServer] = {}

    def run(self) -> None:
        self.install_signal_handlers()
        self.run_worker()
        self.run_manager()
        self.run_monitor()
        self.run_public_server()
        self._loop.run_forever()

    def run_manager(self) -> None:
        def factory():
            session_created_waiter = self._loop.create_future()
            session_disconnect_waiter = self._loop.create_future()

            @session_created_waiter.add_done_callback
            def on_session_created(f: Future) -> None:
                protocol, token, waiter = f.result()  # type: ManagerServer, str, Future
                host_name = protocol.host_name
                self.managers[protocol.host_name] = protocol
                gather = gather_message(
                    self.get_keepers(),
                    Event.CLIENT_CONNECT,
                    token=token,
                    host_name=host_name
                )
                futures._chain_future(gather, waiter)

            @session_disconnect_waiter.add_done_callback
            def on_session_disconnect(f: Future) -> None:
                manager: ManagerServer = f.result()
                host_name = manager.host_name
                if host_name in self.managers:
                    del self.managers[host_name]
                for w in self.workers.values():
                    w.put(
                        Message(
                            Event.CLIENT_DISCONNECT,
                            host_name=host_name
                        )
                    )
            return ManagerServer(session_created_waiter, session_disconnect_waiter, self.workers)

        self._loop.run_until_complete(
            self._loop.create_server(factory, host=settings.manager_bind_host, port=settings.manager_bind_port)
        )
        logger.info(f'manager_server[{settings.manager_bind_host}:{settings.manager_bind_port}] - '
                    f'{os.getpid()} - started')

    def run_public_server(self) -> None:
        for host_name, host, port in settings.client_endpoint_default:
            s = start_public_server(
                ('0.0.0.0', 1200),
                partial(self.receive_public_socket, host_name, (host, port)),
            )
            bind_port = s.getsockname()[1]
            logger.info(f'public server 【{bind_port}】 -> 【{host_name}】({host}:{port})')

    def run_monitor(self):
        return
        self._loop.create_task(
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

    def get_keepers(self) -> List[ProcessMessageKeeper]:
        return [w.message_keeper for w in self.workers.values()]

    async def loop_monitor(self):
        while True:
            from prettytable import PrettyTable
            await asyncio.sleep(0.5)
            Column = ["PID", State.IDLE, State.WORK, State.WAIT_CLIENT_READY, State.WAIT_AUTH, State.DISCONNECT]
            table = PrettyTable(Column)
            table_dict = defaultdict(dict)
            pstr = ["="*50]
            for host_map, pid in await gather_message(
                self.get_keepers(),
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
        for idx in range(settings.workers):
            message_keeper = ProcessMessageKeeper(self, Queue(), Queue())
            pro = get_subprocess(ManagerWorker.run, message_keeper.get_input_queue(),
                                 message_keeper.get_output_queue())
            pro.start()
            # 进程随机port启动proxy服务，这里获取port
            port = message_keeper.get_output_queue().get()
            # for i in range(100):
            #     message_keeper.get_input_queue().put(123)
            message_keeper.listen()
            self.workers[pro.pid] = WorkerStruct(pid=pro.pid, process=pro,
                                                 message_keeper=message_keeper,
                                                 port=port)
            logger.info(f'proxy_server[{settings.proxy_bind_host}:{port}] - {os.getpid()} started')

    def receive_public_socket(self, host_name: str, end_point: tuple, sock: socket.socket) -> None:
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
            # 这里先减去，防止并发导致多个请求进入一个进程，等后面进程重新上报新的数据覆盖
            worker.proxy_state_shot[host_name] -= 1
            # worker.proxy_state_shot[host_name][State.WORK] += 1
            worker.put(
                Message(
                    Event.PUBLIC_CREATE,
                    host_name,
                    sock,
                    end_point
                )
            )
            # MonitorServer.broadcast()
        else:
            print('-------')
            sock.close()
        # print(get_sort_list())

    def on_message_proxy_state_change(self, pid: int, host_name: str, is_idle: bool) -> None:
        self.workers[pid].proxy_state_shot[host_name] += 1 if is_idle else -1
        # MonitorServer.broadcast()

    def install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self.handle_exit)

    def handle_exit(self, sig: signal.Signals, frame: FrameType) -> None:
        self.exit_event.set()


server = Server()

if __name__ == '__main__':
    server.run()
