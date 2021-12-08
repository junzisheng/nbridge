from typing import Union, List
from asyncio import Task, TimerHandle, Future
import asyncio

TaskType = Union[Task, TimerHandle, Future]


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

    def get_set(self) -> List[TaskType]:
        return self._set

    def cancel_all(self):
        for task in self._set:
            if not task.cancelled():
                task.cancel()
        self._set.clear()

    def call_later(self, delay, callback, *args) -> TimerHandlerWrapper:

        def callback_wrapper(*_args):
            self.safe_remove(timer)
            return callback(*_args)
        timer = self._loop.call_later(delay, callback_wrapper, *args)
        self._set.append(timer)
        return TimerHandlerWrapper(timer, self)

    def create_task(self, coro, *, name=None) -> Task:
        task = self._loop.create_task(coro, name=name)
        self._set.append(task)
        task.add_done_callback(lambda _: self.safe_remove(task))
        return task

    def create_future(self) -> Future:
        f = Future()
        f.add_done_callback(
            lambda _: self.safe_remove(f)
        )
        self._set.append(f)
        return f

    def safe_remove(self, f):
        try:
            self._set.remove(f)
        except:
            pass


