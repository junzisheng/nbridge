from typing import Any, Union, Optional, Tuple, Dict, List
import uuid
from multiprocessing import Queue as Mqueue
from multiprocessing.connection import Connection
from asyncio import Queue, Task
import asyncio
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor

from loguru import logger


class MessageType:
    NORMAL = "NORMAL"
    WAITER = "WAITER"
    ANSWER = "ANSWER"
    COLLECT = "COLLECT"
    STOP_LISTEN = "STOP_LISTEN"


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


def broadcast_message(
    keepers: List['MessageKeeper'], event, *args, **kwargs
) -> None:
    for keep in keepers:
        keep.send(Message(event, *args, **kwargs))


class MessageKeeper(object):
    def __init__(self, receiver: Any, input_channel: Union[Queue, Mqueue, Connection],
                 output_channel: Union[Queue, Mqueue, Connection],
                 callback_prefix: str = "on_message_") -> None:
        self.input_channel = input_channel
        self.output_channel = output_channel
        self.receiver = receiver
        self._loop = asyncio.get_event_loop()
        self.callback_prefix = callback_prefix
        self.listen_task: Optional[Task] = None

    def get_input(self) -> Union[Queue, Mqueue, Connection]:
        return self.input_channel

    def get_output(self) -> Union[Queue, Mqueue, Connection]:
        return self.output_channel

    def listen(self) -> None:
        self.listen_task = self._loop.create_task(
            self.async_listen(self.output_channel)
        )

    def send(self, message: Message) -> None:
        self.put(message.dumps())

    def receive(self, message: list) -> None:
        _type = message[0]
        if _type == MessageType.WAITER:
            _, waiter_id, event, args, kwargs = message
            result = self.callback(event, *args, **kwargs)
            if isinstance(result, Future):
                def c(f: Future):
                    self.send(AnswerMessage(waiter_id, f.result()))
                result.add_done_callback(c)
            else:
                self.send(AnswerMessage(waiter_id, result))
        elif _type == MessageType.NORMAL:
            _, event, args, kwargs = message
            self.callback(event, *args, **kwargs)
        elif _type == MessageType.ANSWER:
            _, waiter_id, result = message
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

    def get(self) -> Any:
        raise NotImplementedError

    def put(self, item: Any) -> None:
        raise NotImplementedError

    def should_stop(self, message: list) -> bool:
        from worker import Event
        stop = message[0] == MessageType.NORMAL and message[1] == Event.SERVER_CLOSE
        # 这里需要通知主进程来关闭主进程的循环监听
        if stop:
            self.put(Event.SERVER_CLOSE)
        return stop


class AsyncMessageKeeper(MessageKeeper):
    async def async_listen(self, q: Queue) -> None:
        while True:
            message = await self.get()
            self.receive(message)
            if self.should_stop(message):
                break

    def get(self) -> Any:
        return self.output_channel.get()

    def put(self, item: Any) -> None:
        self.input_channel.put_nowait(item)


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
                    self.get,
                )
                self.receive(message)
                if self.should_stop(message):
                    break
            except Exception as e:
                logger.info(e.__class__.__name__)
        self._executor.shutdown(wait=False)

    def stop(self) -> None:
        super(_ProcessMessageKeeper, self).stop()
        if self._executor:
            self._executor.shutdown(wait=False)


class ProcessQueueMessageKeeper(_ProcessMessageKeeper):
    def get(self) -> Any:
        return self.output_channel.get()

    def put(self, item: Any) -> None:
        self.input_channel.put(item)


class ProcessPipeMessageKeeper(_ProcessMessageKeeper):
    def get(self) -> Any:
        return self.output_channel.recv()

    def put(self, item: Any) -> None:
        self.input_channel.send(item)

    def stop(self):
        super(ProcessPipeMessageKeeper, self).stop()
        self.output_channel.close()
        self.input_channel.close()
