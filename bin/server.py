from typing import Dict
import sys
import signal
from types import FrameType
import threading
import os
import asyncio
from asyncio import Future
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor
from functools import partial

import socket
from loguru import logger
import uvloop
s = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, s)

from sub_process import get_subprocess
from worker import Event, create_event
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
        self.workers: Dict[int, WorkerStruct] = {}
        self._loop = asyncio.get_event_loop()
        self.managers: Dict[str, ManagerServer] = {}
        self._loop.set_default_executor(
            ThreadPoolExecutor(max_workers=max(settings.workers * 2 + 1, 32, (os.cpu_count() or 1)+4))
        )

    def run(self) -> None:
        self.install_signal_handlers()
        self.run_worker()
        self.run_manager()
        self.run_monitor()
        self.run_public_server()

        if True:
            for pid, w in self.workers.items():
                self._loop.create_task(self.listen_queue(pid, w.out_queue))
        self._loop.run_forever()

    def run_manager(self) -> None:
        def factory():
            session_created_waiter = self._loop.create_future()
            session_disconnect_waiter = self._loop.create_future()

            @session_created_waiter.add_done_callback
            def on_session_created(f: Future) -> None:
                protocol, token = f.result()  # type: ManagerServer, str
                host_name = protocol.host_name
                self.managers[protocol.host_name] = protocol
                for w in self.workers.values():
                    w.put(create_event(Event.CLIENT_CONNECT, token=token, host_name=host_name))

            @session_disconnect_waiter.add_done_callback
            def on_session_disconnect(f: Future) -> None:
                manager: ManagerServer = f.result()
                host_name = manager.host_name
                if host_name in self.managers:
                    del self.managers[host_name]
                for w in self.workers.values():
                    w.put(create_event(Event.CLIENT_DISCONNECT, host_name=host_name))
            return ManagerServer(session_created_waiter, session_disconnect_waiter, self.workers)

        self._loop.run_until_complete(
            self._loop.create_server(factory, host=settings.manager_bind_host, port=settings.manager_bind_port)
        )
        logger.info(f'manager_server[{settings.manager_bind_host}:{settings.manager_bind_port}] - '
                    f'{os.getpid()} - started')

    def run_public_server(self) -> None:
        for host_name, host, port in settings.client_endpoint_default:
            s = start_public_server(
                ('0.0.0.0', 1000),
                partial(self.receive_public_socket, host_name, (host, port)),
            )
            bind_port = s.getsockname()[1]
            logger.info(f'public server 【{bind_port}】 -> 【{host_name}】({host}:{port})')

    def run_monitor(self):
        self._loop.run_until_complete(
            self._loop.create_server(
                MonitorServer,
                host=settings.monitor_bind_host,
                port=settings.monitor_bind_port
            )
        )

    def run_worker(self) -> None:
        if True:
            for idx in range(settings.workers):
                input_queue = Queue()
                output_queue = Queue()
                pro = get_subprocess(ManagerWorker.run, input_queue, output_queue)
                pro.start()
                # 进程随机port启动proxy服务，这里获取port
                port = output_queue.get()
                self.workers[pro.pid] = WorkerStruct(pid=pro.pid, process=pro,
                                                     input_queue=input_queue, output_queue=output_queue, port=port)
                logger.info(f'proxy_server[{settings.proxy_bind_host}:{port}] - {os.getpid()} started')
        else:
            raise RuntimeError(f'settings.workers should gather than 1 !')

    def receive_public_socket(self, host_name: str, end_point: tuple, sock: socket.socket) -> None:
        struct_list = list(self.workers.values())
        sort_list = []
        for stru in struct_list:
            sort_list.append(
                [stru.proxy_statistic[host_name].idle,
                 sum(
                     [x.idle for x in stru.proxy_statistic.values()]
                 ), stru.pid]
            )
        sort_list.sort(key=lambda x: [x[0], x[1]], reverse=True)
        receiver = sort_list[0] if sort_list else None
        if receiver and receiver[0] > 0:
            worker = self.workers[receiver[2]]
            print(worker.pid)
            # 这里先减去，防止并发导致多个请求进入一个进程，等后面进程重新上报新的数据覆盖
            worker.proxy_statistic[host_name].idle -= 1
            worker.put(
                create_event(
                    Event.PUBLIC_CREATE,
                    host_name,
                    sock,
                    end_point
                )
            )
        else:
            sock.close()

    async def listen_queue(self, pid, q: Queue) -> None:
        """监听各个进程的任务"""
        while True:
            event, args, kwargs = await self._loop.run_in_executor(None, q.get)  # type: str, tuple, kwargs
            try:
                getattr(self, 'on_' + event.lower())(pid, *args, **kwargs)
            except Exception as e:
                logger.exception(e)

    def on_proxy_state_change(self, pid, host_name: str, idle: int, work: int, state_map: Dict[str, int]) -> None:
        statistic = self.workers[pid].proxy_statistic[host_name]
        statistic.idle = idle
        statistic.work = work
        MonitorServer.broadcast(
            {pid: state_map}
        )

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
