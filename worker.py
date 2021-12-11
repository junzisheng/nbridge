from typing import Tuple, Type, Union
import signal
import os
from multiprocessing.connection import Connection
from multiprocessing import Queue

from loguru import logger

from constants import HANDLED_SIGNALS
from common_bases import Bin
from messager import MessageKeeper
from utils import ignore


class Event(object):
    CLIENT_CONNECT = "CLIENT_CONNECT"
    CLIENT_DISCONNECT = "CLIENT_DISCONNECT"
    PROXY_CREATE = 'PROXY_CREATE'
    PROXY_CONNECTED = "PROXY_CONNECTED"
    PROXY_DISCONNECT = "PROXY_DISCONNECT"
    PROXY_STATE_CHANGE = "PROXY_STATE_CHANGE"
    PUBLIC_CREATE = "PUBLIC_CREATE"
    MANAGER_DISCONNECT = "MANAGER_DISCONNECT"
    QUERY_PROXY_STATE = "QUERY_PROXY_STATE"
    SERVER_CLOSE = "SERVER_CLOSE"
    LISTEN_LOOP_STOP = "LISTEN_LOOP_STOP"


EventType = Tuple[str, tuple, dict]


def create_event(event: str, *args, **kwargs) -> EventType:
    return event, args, kwargs


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
