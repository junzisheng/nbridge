from typing import Any, Union, Optional, Tuple, Dict, List
import uuid
from multiprocessing import Queue as Mqueue
from multiprocessing.connection import Connection
from asyncio import Queue, Task
import asyncio
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor

import os
from loguru import logger


class MessageType:
    NORMAL = "NORMAL"
    WAITER = "WAITER"
    ANSWER = "ANSWER"
    COLLECT = "COLLECT"


class Message(object):
    _type = MessageType.NORMAL

    def __init__(self, event: str, *args, **kwargs) -> None:
        self.event = event
        self.args = args
        self.kwargs = kwargs

    def dumps(self) -> list:
        content = self.get_content()
        content.insert(0, self._type)
        return content

    def get_content(self) -> list:
        return [self.event, self.args, self.kwargs]


class WaiterMessage(Message):
    _type = MessageType.WAITER
    waiter_map: Dict[str, Future] = {}

    def __init__(self, event: str, *args, **kwargs) -> None:
        super(WaiterMessage, self).__init__(event, *args, **kwargs)
        self._waiter = Future()
        self.waiter_id = str(uuid.uuid4())
        self.waiter_map[self.waiter_id] = self._waiter
        self._waiter.add_done_callback(lambda _: self.waiter_map.pop(self.waiter_id))

    def get_waiter(self) -> Future:
        return self._waiter

    def get_content(self) -> list:
        """send: [_type, waiter_id, event, args, kwargs]"""
        """receive: [_type, waiter_id, waiter_result]"""
        content = super(WaiterMessage, self).get_content()
        content.insert(0, self.waiter_id)
        return content

    @classmethod
    def fire(cls, uid, result) -> None:
        waiter = cls.waiter_map.get(uid)
        if waiter and not waiter.cancelled():
            waiter.set_result(result)


class AnswerMessage(Message):
    _type = MessageType.ANSWER

    def __init__(self, waiter_id: str, result: Any) -> None:
        self.waiter_id = waiter_id
        self.result = result

    def get_content(self) -> list:
        return [self.waiter_id, self.result]


class CollectMessage(Message):
    pass


def gather_message(
        keepers: List['MessageKeeper'], event, *args, **kwargs
) -> Future:
    gather = []
    for keep in keepers:
        waiter_message = WaiterMessage(
            event,
            *args,
            **kwargs
        )
        keep.send(waiter_message)
        gather.append(waiter_message.get_waiter())
    return asyncio.gather(*gather)


class MessageKeeper(object):
    def __init__(self, receiver: Any, input: Union[Queue, Mqueue, Connection], output: Union[Queue, Mqueue, Connection],
                 callback_prefix: str = "on_message_") -> None:
        self.input = input
        self.output = output
        self.receiver = receiver
        self._loop = asyncio.get_event_loop()
        self.callback_prefix = callback_prefix
        self.listen_task: Optional[Task] = None

    def get_input(self) -> Union[Queue, Mqueue, Connection]:
        return self.input

    def get_output(self) -> Union[Queue, Mqueue, Connection]:
        return self.output

    def listen(self) -> None:
        self.listen_task = self._loop.create_task(
            self.async_listen(self.output)
        )

    def send(self, message: Message) -> None:
        raise NotImplemented

    def receive(self, message: list) -> None:
        _type = message.pop(0)
        if _type == MessageType.WAITER:
            waiter_id, event, args, kwargs = message
            result = self.callback(event, *args, **kwargs)
            if isinstance(result, Future):
                def c(f: Future):
                    self.send(AnswerMessage(waiter_id, f.result()))
                result.add_done_callback(c)
            else:
                self.send(AnswerMessage(waiter_id, result))
        elif _type == MessageType.NORMAL:
            event, args, kwargs = message
            self.callback(event, *args, **kwargs)
        elif _type == MessageType.ANSWER:
            waiter_id, result = message
            WaiterMessage.fire(waiter_id, result)

    def callback(self, event: str, *args, **kwargs) -> Union[Any, Future]:
        try:
            callback = getattr(self.receiver, self.callback_prefix + event.lower())
            return callback(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            raise

    async def async_listen(self, q: Union[Mqueue, Queue]) -> None:
        raise NotImplementedError

    def stop(self) -> None:
        if self.listen_task and not self.listen_task.done():
            self.listen_task.cancel()

    def do_get(self) -> Any:
        raise NotImplementedError


class AsyncMessageKeeper(MessageKeeper):
    async def async_listen(self, q: Queue) -> None:
        while True:
            message = await self.do_get()
            self.receive(message)

    def send(self, message: Message) -> None:
        self.input.put_nowait(message.dumps())

    def do_get(self) -> Any:
        return self.output.get()


class _ProcessMessageKeeper(MessageKeeper):
    def __init__(self, *args, **kwargs) -> None:
        super(_ProcessMessageKeeper, self).__init__(*args, **kwargs)
        self._executor: Optional[ThreadPoolExecutor] = None

    async def async_listen(self, q: Union[Connection, Queue]) -> None:
        self._executor = ThreadPoolExecutor(max_workers=1)
        while True:
            try:
                message = await self._loop.run_in_executor(
                    self._executor,
                    self.do_get,
                )
            except EOFError:
                # 主进程退出，这里正好退出循环，否则子进程无法正常关闭
                break
            except Exception as e:
                logger.info(e.__class__.__name__)
            else:
                self.receive(message)

    def stop(self) -> None:
        super(_ProcessMessageKeeper, self).stop()
        if self._executor:
            self._executor.shutdown(wait=False)


class ProcessQueueMessageKeeper(_ProcessMessageKeeper):
    def do_get(self) -> Any:
        return self.output.get()

    def send(self, message: Message) -> None:
        self.input.put(message.dumps())


class ProcessPipeMessageKeeper(_ProcessMessageKeeper):
    def do_get(self) -> Any:
        return self.output.recv()

    def send(self, message: Message) -> None:
        self.input.send(message.dumps())

    def stop(self):
        super(ProcessPipeMessageKeeper, self).stop()
        self.output.close()
        self.input.close()
