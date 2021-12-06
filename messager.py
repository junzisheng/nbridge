from typing import Any, Union, Optional, Tuple, Dict
import uuid
from multiprocessing import Queue as Mqueue
from asyncio import Queue, Task
import asyncio
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor


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

    def __init__(self, event: str, waiter: Future, *args, **kwargs) -> None:
        super(WaiterMessage, self).__init__(event, *args, **kwargs)
        self.waiter = waiter
        self.waiter_id = str(uuid.uuid4())
        self.waiter_map[self.waiter_id] = waiter

    def get_content(self) -> list:
        """send: [_type, waiter_id, event, args, kwargs]"""
        """receive: [_type, waiter_id, waiter_result]"""
        content = super(WaiterMessage, self).get_content()
        content.insert(0, self.waiter_id)
        return content

    @classmethod
    def fire(cls, uid, result) -> None:
        cls.waiter_map.pop(uid).set_result(result)


class AnswerMessage(Message):
    _type = MessageType.ANSWER

    def __init__(self, waiter_id: str, result: Any) -> None:
        self.waiter_id = waiter_id
        self.result = result

    def get_content(self) -> list:
        return [self.waiter_id, self.result]


class CollectMessage(Message):
    pass


class MessageKeeper(object):
    def __init__(self, receiver: Any, queue: Union[Queue, Mqueue], callback_prefix: str = "on_message_") -> None:
        self._loop = asyncio.get_running_loop()
        self.receiver = receiver
        self.callback_prefix = callback_prefix
        self.queue = queue
        self.listen_task: Optional[Task] = None

    def get_queue(self) -> Union[Queue, Mqueue]:
        return self.queue

    def listen(self) -> None:
        self.listen_task = self._loop.create_task(
            self.async_listen(self.queue)
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

    def callback(self, event, *args, **kwargs) -> Union[Any, Future]:
        try:
            callback = getattr(self.receiver, self.callback_prefix + event)
        except AttributeError as e:
            raise
        else:
            return callback(*args, **kwargs)

    async def async_listen(self, q: Union[Mqueue, Queue]) -> None:
        raise None

    def stop(self) -> None:
        if self.listen_task and not self.listen_task.done():
            self.listen_task.cancel()


class AsyncMessageKeeper(MessageKeeper):
    async def async_listen(self, q: Queue) -> None:
        while True:
            message = await self.queue.get()
            self.receive(message)

    def send(self, message: Message) -> None:
        self.queue.put_nowait(message.dumps())


class ProcessMessageKeeper(MessageKeeper):
    async def async_listen(self, q: Union[Mqueue, Queue]) -> None:
        _executor = ThreadPoolExecutor(max_workers=1)
        while True:
            try:
                message = await self._loop.run_in_executor(
                    _executor,
                    q.get
                )
            except:
                await asyncio.sleep(0.1)
            else:
                self.receive(message)

    def send(self, message: Message) -> None:
        self.queue.put(message.dumps())
