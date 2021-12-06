import asyncio
import os
import sys

s = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(s)
sys.path.insert(0, s)

from settings import settings
from bridge_manager.monitor import MonitorClient


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        loop.create_connection(
            MonitorClient,
            host=settings.monitor_bind_host,
            port=settings.monitor_bind_port
        )
    )
    loop.run_forever()
