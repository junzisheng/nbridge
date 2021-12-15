from typing import Tuple, Dict, Optional, List, Type, Union
from multiprocessing import Queue
from multiprocessing.connection import Connection
from functools import partial

from loguru import logger

from protocols import Connector
from worker.bases import ProcessWorker
from aexit_context import AexitContext
from utils import safe_remove
from messager import Message, Event, MessageKeeper
from proxy.client import ProxyClient
from config.settings import client_settings


class ClientWorker(ProcessWorker):
    def __init__(
        self, keeper_cls: Type[MessageKeeper], input_channel: Union[Connection, Queue],
        output_channel: Union[Connection, Queue]
    ) -> None:
        super(ClientWorker, self).__init__(keeper_cls, input_channel, output_channel)
        self.proxy_list: List[ProxyClient] = []
        # self.manager_connected = False
        # self.running_exit = AexitContext()

    async def _handle_stop(self) -> None:
        pass

    def on_message_proxy_apply(self, epoch: int, port: int) -> None:
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
                on_session_made,
                on_lost,
                epoch
            ),
            host=client_settings.server_host,
            port=port,
        )
        connector.connect()

        @self.aexit.add_callback_when_cancel_all
        def _():
            connector.abort()
            for proxy in self.proxy_list:
                proxy.transport.close()
            self.proxy_list = []

    def on_message_manager_session_made(self) -> None:
        pass

    def on_message_manager_session_lost(self) -> None:
        self.aexit.cancel_all()

