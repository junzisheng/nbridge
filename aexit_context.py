from typing import Union, List
from asyncio import Task, TimerHandle, Future
import asyncio

from utils import safe_remove

TaskType = Union[Task, TimerHandle, Future, 'AexitContext']


class TimerHandlerWrapper(object):
    def __init__(self, timer: TimerHandle, context: 'AexitContext') -> None:
        self.__timer = timer
        self.__context = context

    def cancel(self) -> None:
        self.__context.get_set().remove(self.__timer)
        self.__timer.cancel()

    def __getattr__(self, item):
        return getattr(self.__timer, item)


class AexitContext(object):
    def __init__(self) -> None:
        self._set: List[TaskType] = []
        self._loop = asyncio.get_event_loop()

    def add_callback_when_cancel_all(self, cancel: callable) -> None:
        f = self.create_future()

        @f.add_done_callback
        def _(_):
            cancel()

    def mount(self, aexit: 'AexitContext') -> None:
        self.add_callback_when_cancel_all(aexit.cancel_all)

    def get_set(self) -> List[TaskType]:
        return self._set

    def cancel_all(self):
        for task in self._set:
            if not task.cancelled():
                task.cancel()
        self._set.clear()

    def monitor_future(self, f: Future) -> None:
        self._set.append(f)
        f.add_done_callback(lambda _: safe_remove(self._set, f))

    cancel = cancel_all

    def call_later(self, delay, callback, *args) -> TimerHandlerWrapper:

        def callback_wrapper(*_args):
            safe_remove(self._set, timer)
            return callback(*_args)
        timer = self._loop.call_later(delay, callback_wrapper, *args)
        self._set.append(timer)
        return TimerHandlerWrapper(timer, self)

    def create_task(self, coro, *, name=None) -> Task:
        task = self._loop.create_task(coro, name=name)
        self._set.append(task)
        task.add_done_callback(lambda _: safe_remove(self._set, task))
        return task

    def create_future(self) -> Future:
        f = Future()
        f.add_done_callback(
            lambda _: safe_remove(self._set, f)
        )
        self._set.append(f)
        return f



