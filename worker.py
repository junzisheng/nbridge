from typing import Tuple
import asyncio
from multiprocessing import Queue

import os
from loguru import logger

from messager import ProcessMessageKeeper


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


EventType = Tuple[str, tuple, dict]


def create_event(event: str, *args, **kwargs) -> EventType:
    return event, args, kwargs


class ProcessWorker(object):
    def __init__(self, output_queue: Queue, input_queue: Queue) -> None:
        self._loop = asyncio.get_event_loop()
        self.pid = os.getpid()
        self.message_keeper = ProcessMessageKeeper(self, input_queue, output_queue)

    @classmethod
    def run(cls, *args, **kwargs) -> None:
        worker = cls(*args, **kwargs)
        worker.message_keeper.listen()
        worker.start()
        worker._loop.run_forever()

    def start(self) -> None:
        pass
