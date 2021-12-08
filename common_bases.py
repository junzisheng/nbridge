from typing import Callable, Coroutine
from asyncio import Event
import asyncio


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

