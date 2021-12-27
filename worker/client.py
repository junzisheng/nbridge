from typing import Dict, List, Type, Union
from multiprocessing import Queue
from multiprocessing.connection import Connection
from functools import partial

from loguru import logger

from protocols import Connector
from worker.bases import ProcessWorker
from utils import safe_remove
from messager import Message, Event, MessageKeeper
from proxy.client import ProxyClient
from config.settings import ClientSettings


class ClientWorker(ProcessWorker):
    def __init__(
        self, client_settings: ClientSettings, keeper_cls: Type[MessageKeeper], input_channel: Union[Connection, Queue],
        output_channel: Union[Connection, Queue]
    ) -> None:
        super(ClientWorker, self).__init__(keeper_cls, input_channel, output_channel)
        self.client_settings = client_settings
        self.proxy_list: List[ProxyClient] = []

    async def _handle_stop(self) -> None:
        pass

    def make_receiver(self) -> Dict:
        def proxy_create(epoch: int, port: int) -> None:
            def on_session_made(proxy: ProxyClient) -> None:
                self.proxy_list.append(proxy)

            def on_lost(proxy: ProxyClient) -> None:
                safe_remove(self.proxy_list, proxy)
                self.message_keeper.send(Message(
                    Event.PROXY_LOST,
                    self.pid,
                    port
                ))

            connector = Connector(
                partial(
                    ProxyClient,
                    self.client_settings,
                    on_session_made,
                    on_lost,
                    epoch
                ),
                host=self.client_settings.server_host,
                port=port,
            )
            connector.connect()

            @self.aexit.add_callback_when_cancel_all
            def _():
                connector.abort()
                for proxy in self.proxy_list:
                    proxy.transport.close()
                self.proxy_list = []

        def manager_session_made() -> None:
            pass

        def manager_session_lost() -> None:
            self.aexit.cancel_all()

        return {
            Event.MANAGER_SESSION_MADE: manager_session_made,
            Event.MANAGER_SESSION_LOST: manager_session_lost,
            Event.PROXY_CREATE: proxy_create
        }


