from typing import Tuple, Dict, Optional, List, Type, Union
import signal
from multiprocessing import Process
import time
import os
from collections import defaultdict
from multiprocessing.connection import Connection
from asyncio import Queue, Future
import socket

from loguru import logger

from constants import HANDLED_SIGNALS
from common_bases import Bin, Client
from messager import MessageKeeper
from utils import ignore
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

    async def _handle_stop(self) -> None:
        raise NotImplementedError

    def on_message_server_close(self) -> None:
        self.closer.call_close()

    def install_signal_handlers(self) -> None:
        """进程不监听信号，接收父进程消息结束进程"""
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, ignore)


class ProxyStateWrapper(ProxyServer):
    state = ProxyState.INIT
    last_idle: Optional[int] = None

    def state_to_idle(self) -> None:
        assert self.state in [ProxyState.INIT, ProxyState.BUSY]
        self.last_idle = int(time.time())
        self.state = ProxyState.IDLE

    def state_to_busy(self):
        assert self.state == ProxyState.IDLE
        self.state = ProxyState.BUSY

    def is_idle(self) -> bool:
        return self.state == ProxyState.IDLE

    def is_busy(self) -> bool:
        return self.state == ProxyState.BUSY


class ProxyPool(Queue):
    def __init__(self):
        super(ProxyPool, self).__init__()
        self.busy: List[ProxyStateWrapper] = []
        self._close_done_waiter: Optional[Future] = None

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
        return proxy

    def _put(self, proxy: ProxyStateWrapper) -> None:
        self._check_closing()
        if proxy.is_busy():
            self.remove(proxy)
        proxy.state_to_idle()
        super(ProxyPool, self)._put(proxy)
        self.log()

    def remove(self, proxy: ProxyStateWrapper) -> None:
        if proxy.is_idle():
            self._queue.remove(proxy)
        else:
            self.busy.remove(proxy)
            if self.closing():
                self._check_all_closed()
        self.log()
        self.may_apply_more()

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

    def may_apply_more(self) -> None:
        if not self.closing():
            pass

    def _check_all_closed(self) -> None:
        if not self.busy:
            self._close_done_waiter.set_result(None)

    def log(self) -> None:
        print(f'[{os.getpid()}] - idle: {self.qsize()} - busy: {len(self.busy)}')


class ClientStruct(object):
    def __init__(self, name: str, token: str, public_sockets: List[socket.socket]) -> None:
        self.name = name
        self.token = token
        self.session: bool = False
        self.public_read: bool = False
        self.public_sockets = public_sockets
        self.proxy_pool = ProxyPool()
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
