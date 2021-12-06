import asyncio
import pytest

from aexit_context import AexitContext


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [0, 1])
async def test_context_later(event_loop, cancelled) -> None:
    ac = AexitContext()
    _t = 0

    def t():
        nonlocal _t
        _t += 1
    timer = ac.call_later(.1, t)
    if cancelled == 0:
        timer.cancel()

    await asyncio.sleep(.2)
    assert _t == cancelled
    assert len(ac.get_set()) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [0, 1])
async def test_context_create_task(event_loop, cancelled) -> None:
    ac = AexitContext()
    _t = 0

    async def task_test():
        nonlocal _t
        _t += 1
    task = ac.create_task(task_test())
    if cancelled == 0:
        task.cancel()
        await asyncio.sleep(.1)
    else:
        await task
    assert _t == cancelled
    assert len(ac.get_set()) == 0