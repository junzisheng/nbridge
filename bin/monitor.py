import asyncio
import os
import sys

s = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, s)

from loguru import logger

from settings import settings
from manager.monitor import MonitorClient

loop = asyncio.get_event_loop()


async def connect(factory):
    try:
        await loop.create_connection(
            factory,
            host=settings.monitor_bind_host,
            port=settings.monitor_bind_port
        )
    except Exception as e:
        await asyncio.sleep(1)
        logger.debug('retry')
        loop.create_task(connect(factory))


async def run():

    def factory():
        f = asyncio.Future()

        @f.add_done_callback
        def callback(_):
            loop.create_task(connect(factory))

        return MonitorClient(f)
    await connect(factory)


if __name__ == '__main__':
    loop.run_until_complete(
        run()
    )
    loop.run_forever()
