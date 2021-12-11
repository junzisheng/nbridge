from typing import Optional
import asyncio
from asyncio import Future, CancelledError

from loguru import logger

from config.settings import client_settings
from common_bases import Bin
from manager.monitor import MonitorClient, MonitorConnector

loop = asyncio.get_event_loop()


class Monitor(Bin):
    def __init__(self):
        super(Monitor, self).__init__()
        self.client: Optional[MonitorClient] = None

    def start(self):
        connector = MonitorConnector()
        connector.connect(
            MonitorClient,
            host=client_settings.server_host,
            port=client_settings.monitor_port
        )

        def pre_garbage():
            f = self.aexit.create_future()
            connector.set_waiter(f)

            @f.add_done_callback
            def on_close(r: Future):
                try:
                    client = r.result()
                except CancelledError:
                    connector.abort()
                else:
                    self.client = client
                    if connector.mode == 'always':
                        pre_garbage()
        pre_garbage()

    async def do_handle_stop(self) -> None:
        if self.client:
            self.client.transport.close()
        await asyncio.sleep(0)


if __name__ == '__main__':
    Monitor().run()


