import asyncio
import pytest
from asyncio import Queue, Future
from multiprocessing import Queue as Mqueue, Pipe

from messager import AsyncMessageKeeper, ProcessQueueMessageKeeper, ProcessPipeMessageKeeper, \
    Message, WaiterMessage, gather_message


def _make_parametrize():
    mq1 = Mqueue()
    mq2 = Mqueue()
    aq1 = Queue()
    aq2 = Queue()
    p1, p2 = Pipe()
    p3, p4 = Pipe()

    return [
        [ProcessQueueMessageKeeper, mq1, mq1, mq2, mq2],
        [AsyncMessageKeeper, aq1, aq1, aq2, aq2],
        [ProcessPipeMessageKeeper, p1, p2, p3, p4],
    ]


class Receiver(object):
    def __init__(self):
        self.waiter = Future()

    def on_message_test(self, a, b):
        self.waiter.set_result(a+b)

    def on_message_test_imm(self, a, b):
        return a + b

    def on_message_test_async(self, a, b):
        f = Future()
        asyncio.get_running_loop().call_later(.1, f.set_result, a+b)
        return f


@pytest.mark.asyncio
@pytest.mark.parametrize('keeper_opt', _make_parametrize())
async def test_async_message(event_loop, keeper_opt):
    receiver = Receiver()
    keep_cls, server_input, server_output, client_input, client_output = keeper_opt
    server_keeper = keep_cls(receiver, server_input, client_output)
    client_keeper = keep_cls(receiver, client_input, server_output)
    server_keeper.listen()
    client_keeper.listen()
    # test normal message
    server_keeper.send(Message('test', 1, 2))
    assert await receiver.waiter == 3
    # test waiter imm
    waiter_message = WaiterMessage('test_imm', 1, 2)
    server_keeper.send(waiter_message)
    assert await waiter_message.get_waiter() == 3
    # test waiter async
    waiter_message = WaiterMessage('test_async', 1, 2)
    server_keeper.send(waiter_message)
    assert await waiter_message.get_waiter() == 3


def _make_gather_parametrize():
    return [
        (ProcessQueueMessageKeeper, lambda: [Mqueue()] * 2),
        (AsyncMessageKeeper, lambda: [Queue()] * 2),
        (ProcessPipeMessageKeeper, Pipe),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize('keeper_opt', _make_gather_parametrize())
async def test_gather_message(event_loop, keeper_opt):
    receiver = Receiver()
    keeper_cls, q_func = keeper_opt
    keeper_list = []
    ranges = 5
    for i in range(ranges):
        keeper = keeper_cls(receiver, *q_func())
        keeper_list.append(keeper)
        keeper.listen()
    assert [3] * ranges == await gather_message(
        keeper_list,
        'test_imm',
        1,
        2
    )
    assert [6] * ranges == await gather_message(
        keeper_list,
        'test_async',
        4,
        2
    )
