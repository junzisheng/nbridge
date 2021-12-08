from typing import List, Optional
from asyncio import Queue

from loguru import logger

from revoker import Revoker, AuthRevoker, PingPongRevoker
from protocols import BaseProtocol
from settings import settings
from worker import Event, create_event


class ClientRevoker(Revoker):
    authed = False

    def call_session_created(self, token: str, proxy_port_list: List[int]) -> None:
        self.authed = True
        logger.info(f'【{self.protocol.host_name}】Manager Server Session Created')
        for port in proxy_port_list:
            self.protocol.queue.put_nowait(create_event(
                Event.PROXY_CREATE,
                port=port,
                token=token
            ))


class ClientProtocol(BaseProtocol):
    revoker_bases = (ClientRevoker, PingPongRevoker)

    def __init__(self, queue: Queue, host_name: str) -> None:
        super().__init__()
        self.queue = queue
        self.host_name = host_name

    def on_connection_made(self) -> None:
        self.rpc_call(
            AuthRevoker.call_auth,
            settings.token,
            self.host_name
        )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.revoker.authed:
            self.queue.put_nowait(
                create_event(
                    Event.MANAGER_DISCONNECT,
                    self.close_reason
                )
            )


