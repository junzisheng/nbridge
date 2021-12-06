import os
import asyncio

import pytest
import uvloop

uvloop.install()


os.environ['pytest'] = 'True'


@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop


