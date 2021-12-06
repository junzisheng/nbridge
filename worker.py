from typing import Tuple
import asyncio
from multiprocessing import Queue

from loguru import logger


class Event(object):
    CLIENT_CONNECT = "CLIENT_CONNECT"
    CLIENT_DISCONNECT = "CLIENT_DISCONNECT"
    PROXY_CREATE = 'PROXY_CREATE'
    PROXY_CONNECTED = "PROXY_CONNECTED"
    PROXY_DISCONNECT = "PROXY_DISCONNECT"
    PROXY_STATE_CHANGE = "PROXY_STATE_CHANGE"
    PUBLIC_CREATE = "PUBLIC_CREATE"
    MANAGER_DISCONNECT = "MANAGER_DISCONNECT"


EventType = Tuple[str, tuple, dict]


def create_event(event: str, *args, **kwargs) -> EventType:
    return event, args, kwargs


class ProcessWorker(object):
    def __init__(self, output_queue: Queue, input_queue: Queue) -> None:
        self._loop = asyncio.get_event_loop()
        self.input_queue = input_queue
        self.output_queue = output_queue

    @classmethod
    def run(cls, *args, **kwargs) -> None:
        worker = cls(*args, **kwargs)
        worker._loop.create_task(
            worker.listen_queue()
        )
        worker.start()
        worker._loop.run_forever()

    def start(self) -> None:
        pass

    async def listen_queue(self) -> None:
        while True:
            try:
                event, args, kwargs = await \
                    self._loop.run_in_executor(None, self.output_queue.get)  # type: str, tuple, dict
            except ConnectionRefusedError:
                continue
            try:
                getattr(self, 'on_'+event.lower())(*args, **kwargs)
            except Exception as e:
                logger.exception(e)

