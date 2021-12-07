import pytest
from asyncio import Queue, Future
from multiprocessing import Queue as Mqueue

from messager import AsyncMessageKeeper, ProcessMessageKeeper, Message, WaiterMessage, gather_message


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
    keeper = keeper_cls(Receiver(f), q, q)
    keeper.listen()
    keeper.send(Message('test', 1, b=2))
    assert await f == 3
    keeper.stop()


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
    keeper = keeper_cls(Receiver(), q, q)
    keeper.listen()
    waiter_message = WaiterMessage(
        'return_imm',
        1,
        b=2
    )
    keeper.send(waiter_message)
    waiter_message_2 = WaiterMessage(
        'return_async',
        1,
        b=3
    )
    keeper.send(waiter_message_2)
    assert await waiter_message.get_waiter() == 3
    assert await waiter_message_2.get_waiter() == 4
    assert WaiterMessage.waiter_map == {}
    keeper.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize('keeper_opt',
                         [(ProcessMessageKeeper, Mqueue), (AsyncMessageKeeper, Queue)]
                         )
async def test_gather_message(event_loop, keeper_opt):
    class Receiver(object):
        g = 0

        def on_message_test_gather(self, start: int):
            self.g += start
            return self.g

    keeper_cls, q_cls = keeper_opt
    keeper_list = []
    receiver = Receiver()
    for i in range(5):
        q = q_cls()
        keeper = keeper_cls(receiver, q, q)
        keeper_list.append(keeper)
        keeper.listen()
    result = await gather_message(keeper_list, 'test_gather', 5)
    assert result == [5, 10, 15, 20, 25]
    for keeper in keeper_list:
        keeper.stop()







