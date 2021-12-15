from typing import Callable, Coroutine, Optional
import signal
import threading
from asyncio import Event
import asyncio
from aexit_context import AexitContext

import uvloop
from loguru import logger
from pydantic import BaseModel

from constants import HANDLED_SIGNALS


class Closer(object):
    def __init__(self, handle_stop: Callable[..., Coroutine]) -> None:
        self._close_event = Event()
        self.handle_stop = handle_stop

    def run(self):
        asyncio.get_event_loop().create_task(
            self.while_stop()
        )

    def call_close(self) -> None:
        self._close_event.set()

    async def while_stop(self) -> None:
        await self._close_event.wait()
        await self.handle_stop()


class Bin(object):
    def __init__(self):
        uvloop.install()
        self._loop = asyncio.get_event_loop()
        self.closer = Closer(self._handle_close)
        self.aexit = AexitContext()
        self._stop = False

    def run(self) -> None:
        self.closer.run()
        self.install_signal_handlers()
        self.start()
        self._loop.run_forever()
        self._loop.close()

    def start(self) -> None:
        raise NotImplementedError

    def install_signal_handlers(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            return
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self._handle_exit)

    def _handle_exit(self, *args, **kwargs) -> None:
        self._stop = True
        self.closer.call_close()

    async def _handle_close(self) -> None:
        self.aexit.cancel_all()
        await self.do_handle_stop()
        self._loop.stop()

    async def do_handle_stop(self) -> None:
        raise NotImplementedError


class Forwarder(object):
    forwarder: Optional['Forwarder'] = None
    forwarder_status = -1   # 0 初始化 1 工作 -1 关闭
    forwarder_buffer = b''

    def reset_forwarder(self) -> None:
        assert self.forwarder_status == -1
        self.forwarder_status = 0
        self.forwarder_buffer = b''
        self.forwarder = None

    def set_forwarder(self, forwarder: 'Forwarder') -> None:
        assert self.forwarder_status == 0
        self.forwarder_status = 1
        self.forwarder = forwarder
        if self.forwarder_buffer:
            self.forward(self.forwarder_buffer)
            self.forwarder_buffer = b''

    def forward(self, data: bytes) -> None:
        if self.forwarder_status == 0:
            self.forwarder_buffer += data
        elif self.forwarder_status == 1:
            self.forwarder.write(data)

    def write(self, data: bytes) -> None:
        raise NotImplementedError

    def close_forwarder(self) -> None:
        if self.forwarder_status == 1:
            self.forwarder.forwarder_status = -1
            self.forwarder.forwarder_buffer = b''
            self.forwarder.on_forwarder_close()
            self.forwarder.forwarder = None
        self.forwarder_buffer = b''
        self.forwarder_status = -1
        self.forwarder = None

    def on_forwarder_close(self) -> None:
        pass


class Client(BaseModel):
    name: str
    token: str
    epoch: int = 0

    def increase_epoch(self) -> int:
        self.epoch += 1
        return self.epoch


class Public(BaseModel):
    client: Client
    type: str
    name: str
    local_host: str
    local_port: int
    bind_port: int = 0


