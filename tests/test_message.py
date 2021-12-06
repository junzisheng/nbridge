import pytest
from asyncio import Queue, Future
from multiprocessing import Queue as Mqueue

from messager import AsyncMessageKeeper, ProcessMessageKeeper, Message, WaiterMessage


@pytest.mark.asyncio
@pytest.mark.parametrize('keeper_opt',
                         [(ProcessMessageKeeper, Mqueue()),
                          (AsyncMessageKeeper, Queue())]
                         )
async def test_async_message(event_loop, keeper_opt):
    class Receiver(object):
        def __init__(self, f: Future):
            self.waiter = f

        def on_message_test(self, a, b):
            self.waiter.set_result(a + b)

    f = Future()
    keeper_cls, q = keeper_opt
    keeper = keeper_cls(Receiver(f), q)
    keeper.listen()
    keeper.send(Message('test', 1, b=2))
    assert await f == 3


@pytest.mark.asyncio
@pytest.mark.parametrize('keeper_opt',
                         [(ProcessMessageKeeper, Mqueue()),
                          (AsyncMessageKeeper, Queue())]
                         )
async def test_waiter_message(event_loop, keeper_opt):
    class Receiver(object):
        @staticmethod
        def on_message_return_imm(a, b):
            return a + b

        @staticmethod
        def on_message_return_async(a, b):
            f = Future()
            event_loop.call_later(.1, f.set_result, a+b)
            return f
    keeper_cls, q = keeper_opt
    keeper = keeper_cls(Receiver(), q)
    keeper.listen()
    f1 = Future()
    f2 = Future()
    keeper.send(WaiterMessage(
        'return_imm',
        f1,
        1,
        b=2
    ))
    keeper.send(WaiterMessage(
        'return_async',
        f2,
        1,
        b=3
    ))
    assert await f1 == 3
    assert await f2 == 4
    assert WaiterMessage.waiter_map == {}





