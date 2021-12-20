from typing import Optional
import asyncio
from asyncio import Future, CancelledError

from loguru import logger

from common_bases import Bin
from manager.monitor import MonitorClient, MonitorConnector

loop = asyncio.get_event_loop()


class Monitor(Bin):
    def __init__(self):
        super(Monitor, self).__init__()
        self.client: Optional[MonitorClient] = None

    def start(self):
        connector = MonitorConnector(
            MonitorClient,
            host=client_settings.server_host,
            port=client_settings.monitor_port
        )
        connector.connect()
        self.aexit.add_callback_when_cancel_all(connector.abort)

    async def do_handle_stop(self) -> None:
        if self.client:
            self.client.transport.close()
        await asyncio.sleep(0)


if __name__ == '__main__':
    Monitor().run()


