from typing import Dict, Optional, List, Type, Union
import asyncio
from asyncio import Task
import signal
from multiprocessing import Process
import os
from collections import defaultdict
from multiprocessing.connection import Connection
from asyncio import Queue, Future
import socket

from loguru import logger

from constants import HANDLED_SIGNALS
from common_bases import Bin, Client
from messager import MessageKeeper
from utils import ignore, catch_cor_exception
from constants import CloseReason, ProxyState
from aexit_context import AexitContext
from messager import Message, ProcessPipeMessageKeeper
from proxy.server import ProxyServer


class WorkerStruct(object):
    def __init__(
        self, process: Process, message_keeper: ProcessPipeMessageKeeper
    ) -> None:
        self.pid = process.pid
        self.process = process
        self.name = 'worker-%s' % self.pid
        self.message_keeper = message_keeper

    def put(self, message: Message) -> None:
        self.message_keeper.send(message)
        

class ServerWorkerStruct(WorkerStruct):
    def __init__(self, proxy_port: int, *args, **kwargs) -> None:
        super(ServerWorkerStruct, self).__init__(*args, **kwargs)
        self.proxy_port = proxy_port


class ClientWorkerStruct(WorkerStruct):
    def __init__(self, *args, **kwargs) -> None:
        super(ClientWorkerStruct, self).__init__(*args, **kwargs)
        self.proxy_state: Dict[int, int] = defaultdict(lambda :0)


def run_worker(worker_cls: Type['ProcessWorker'], *args, **kwargs) -> None:
    # 这里忽略信号，由主进程通知关闭
    worker = worker_cls(*args, **kwargs)
    worker.message_keeper.listen()
    # worker.closer.run()
    worker.run()
    # worker._loop.run_forever()
    # worker._loop.close()


class ProcessWorker(Bin):
    def __init__(self, keeper_cls: Type[MessageKeeper],
                 input_channel: Union[Connection, Queue], output_channel: Union[Connection, Queue]) -> None:
        super(ProcessWorker, self).__init__()
        self.pid = os.getpid()
        self.message_keeper = keeper_cls(self, input_channel, output_channel)

    def start(self) -> None:
        pass

    async def do_handle_stop(self) -> None:
        self.message_keeper.stop()
        self.message_keeper.close()
        await self._handle_stop()
        await asyncio.sleep(0)

    async def _handle_stop(self) -> None:
        raise NotImplementedError

    def on_message_server_close(self) -> None:
        self.closer.call_close()

    def install_signal_handlers(self) -> None:
        """进程不监听信号，接收父进程消息结束进程"""
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, ignore)


_id = 0
class ProxyStateWrapper(ProxyServer):
    state = ProxyState.INIT
    last_idle: Optional[float] = None

    def __init__(self, *args, **kwargs):
        global _id
        _id += 1
        super(ProxyStateWrapper, self).__init__(*args, **kwargs)
        self._id = _id

    def state_to_idle(self) -> None:
        assert self.state in [ProxyState.INIT, ProxyState.BUSY]
        print(self._id, 'to_idle')
        self.last_idle = self._loop.time()
        self.state = ProxyState.IDLE

    def state_to_busy(self):
        assert self.state == ProxyState.IDLE
        print(self._id, 'to_busy')
        self.state = ProxyState.BUSY

    def is_idle(self) -> bool:
        return self.state == ProxyState.IDLE

    def is_busy(self) -> bool:
        return self.state == ProxyState.BUSY


class ProxyPool(Queue):
    def __init__(self, loop, async_create_proxy_invoke, min_size: int):
        super(ProxyPool, self).__init__()
        self.loop = loop
        self.busy: List[ProxyStateWrapper] = []
        self.async_create_proxy_invoke = async_create_proxy_invoke
        self._close_done_waiter: Optional[Future] = None
        self.min_size = min_size
        self.recycle_task: Optional[Task] = None

    def start_recycle_task(self):
        from config.settings import server_settings
        loop = self.loop

        @catch_cor_exception
        async def recycle():
            while True:
                await asyncio.sleep(1)
                recycle_list = []
                now = loop.time()
                recycle_num = self.qsize() - self.min_size
                if recycle_num > 0:
                    for proxy in self._queue:  # type: ProxyStateWrapper
                        if now - proxy.last_idle > server_settings.proxy_pool_recycle:
                            if len(recycle_list) != recycle_num:
                                recycle_list.append(proxy)
                            else:
                                break
                    while recycle_list:
                        proxy = recycle_list.pop()
                        proxy.close(CloseReason.PROXY_RECYCLE)

        self.recycle_task = loop.create_task(recycle())

    def closing(self) -> bool:
        return self._close_done_waiter is not None

    def _check_closing(self) -> None:
        assert self._close_done_waiter is None

    def _get(self) -> ProxyStateWrapper:
        self._check_closing()
        proxy = super(ProxyPool, self)._get()
        proxy.state_to_busy()
        self.busy.append(proxy)
        self.log()
        self.may_create_more()
        return proxy

    def _put(self, proxy: ProxyStateWrapper) -> None:
        try:
            assert proxy not in self._queue
        except:
            print('----')
        self._check_closing()
        if proxy.is_busy():
            self.remove(proxy)
        proxy.state_to_idle()
        super(ProxyPool, self)._put(proxy)
        self.log()

    def remove(self, proxy: ProxyStateWrapper) -> None:
        if proxy.is_idle():
            self._queue.remove(proxy)
            self.may_create_more()
        else:
            self.busy.remove(proxy)
            if self.closing():
                self._check_all_closed()
        self.log()

    def close_all(self, reason: int) -> Future:
        while not self.empty():
            proxy = self.get_nowait()  # type: ProxyStateWrapper
            proxy.close(reason)

        while self._getters:
            getter = self._getters.popleft()  # type: Future
            getter.cancel()

        for proxy in self.busy:
            proxy.transport.close()
        self._close_done_waiter = self._loop.create_future()  # type: Future

        @self._close_done_waiter.add_done_callback
        def _(_):
            self._close_done_waiter = None
        self._check_all_closed()
        return self._close_done_waiter

    def may_create_more(self) -> None:
        if not self.closing():
            if self.qsize() < self.min_size:
                self.async_create_proxy_invoke(1)

    def _check_all_closed(self) -> None:
        if not self.busy:
            self._close_done_waiter.set_result(None)

    def log(self) -> None:
        # pass
        print(f'[{os.getpid()}] - idle: {self.qsize()} - busy: {len(self.busy)}')


class ClientStruct(object):
    def __init__(self, name: str, token: str, public_sockets: List[socket.socket], proxy_pool: ProxyPool) -> None:
        self.name = name
        self.token = token
        self.session: bool = False
        self.public_read: bool = False
        self.public_sockets = public_sockets
        self.proxy_pool = proxy_pool
        self.aexit = AexitContext()

    @property
    def closed(self) -> bool:
        return not self.session

    def open_session(self, epoch) -> None:
        assert not self.proxy_pool.closing()
        self.session = True
        self.epoch = epoch

    def close_session(self) -> Future:
        self.session = False
        self.epoch = -1
        return self.proxy_pool.close_all(CloseReason.MANAGE_CLIENT_LOST)
