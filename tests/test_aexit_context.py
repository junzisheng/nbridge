import asyncio
from asyncio import Future

import pytest

from aexit_context import AexitContext
from utils import async_ignore, ignore


class TestAexitContext(object):
    aexit = AexitContext()
    cancelled = True

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cancel', [False, True])
    async def test_call_later(self, event_loop, cancel):
        call_later_delay = 0.1

        def later():
            self.cancelled = False
        timer_wrapper = self.aexit.call_later(call_later_delay, later)
        assert timer_wrapper.get_timer() in self.aexit.get_set()
        if cancel:
            timer_wrapper.cancel()
        await asyncio.sleep(call_later_delay)
        assert not self.aexit.get_set()
        assert self.cancelled == cancel

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cancel', [False, True])
    async def test_create_task(self, event_loop, cancel):
        async def t():
            self.cancelled = False
        task = self.aexit.create_task(t())
        if cancel:
            task.cancel()
        assert task in self.aexit.get_set()
        await asyncio.sleep(0)
        assert self.cancelled == cancel
        await asyncio.sleep(0)
        assert not self.aexit.get_set()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cancel', [False, True])
    async def test_create_future(self, event_loop, cancel):
        def callback(f: Future):
            self.cancelled = f.cancelled()
        f = self.aexit.create_future()
        f.add_done_callback(callback)
        if cancel:
            f.cancel()
        else:
            f.set_result(None)
        assert f in self.aexit.get_set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert self.cancelled == cancel
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert not self.aexit.get_set()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cancel', [False, True])
    async def test_cancel_all(self, event_loop, cancel):
        f = self.aexit.create_future()
        task = self.aexit.create_task(async_ignore())
        timer = self.aexit.call_later(10, ignore)
        self.aexit.cancel_all()
        await asyncio.sleep(0)
        assert [f.cancelled(), task.cancelled(), timer.get_timer().cancelled()] == [True] * 3
        assert not self.aexit.get_set()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cancel', [False, True])
    async def test_monitor_future(self, event_loop, cancel):
        f = event_loop.create_future()
        self.aexit.monitor_future(f)
        assert f in self.aexit.get_set()
        if cancel:
            f.cancel()
        else:
            f.set_result(None)
        await asyncio.sleep(0)
        assert not self.aexit.get_set()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cancel', [False, True])
    async def test_callback_when_cancel_all(self, event_loop, cancel):
        cancelled = False

        def on_cancel():
            nonlocal cancelled
            cancelled = True
        self.aexit.add_callback_when_cancel_all(on_cancel)
        assert len(self.aexit.get_callbacks()) == 1
        self.aexit.cancel_all()
        await asyncio.sleep(0)
        assert cancelled
        self.aexit.clear_callbacks()
        assert not self.aexit.get_callbacks()

    @pytest.mark.asyncio
    @pytest.mark.parametrize('cancel', [False, True])
    async def test_mount(self, event_loop, cancel):
        res = []

        def cancel_callback(_f: Future):
            if _f.cancelled():
                res.append(1)
        mount_aexit = AexitContext()
        # create future
        mount_aexit.create_future().add_done_callback(cancel_callback)
        # create_task
        task = mount_aexit.create_task(asyncio.sleep(1))
        task.add_done_callback(cancel_callback)
        # monitor_future
        f = Future()
        f.add_done_callback(cancel_callback)
        mount_aexit.monitor_future(f)
        # add_callback_when_cancel_all
        f2 = Future()
        f2.add_done_callback(cancel_callback)
        mount_aexit.add_callback_when_cancel_all(f2.cancel)
        self.aexit.mount(mount_aexit)
        assert len(self.aexit.get_callbacks()) == 1
        self.aexit.cancel_all()
        await asyncio.sleep(0)
        assert sum(res) == 3
        await asyncio.sleep(0)  # for task cancel
        assert sum(res) == 4
        self.aexit.clear_callbacks()


