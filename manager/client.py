from typing import List, Optional
from asyncio import Queue

from loguru import logger

from revoker import Revoker, AuthRevoker, PingPongRevoker
from protocols import BaseProtocol
from config.settings import client_settings


class ClientRevoker(Revoker):
    authed = False

    def call_session_created(self, token: str, proxy_ports: List[int], proxy_size: int) -> None:
        self.authed = True
        logger.info(f'【{client_settings.name}】Manager Server Session Created')
        self.protocol.on_manager_session_made(
            token,
            proxy_ports,
            proxy_size
        )


class ClientProtocol(BaseProtocol):
    revoker_bases = (ClientRevoker, PingPongRevoker)

    def __init__(self, *, on_manager_session_made: callable, on_manager_session_lost: callable) -> None:
        super().__init__()
        self.on_manager_session_made = on_manager_session_made
        self.on_manager_session_lost = on_manager_session_lost

    def on_connection_made(self) -> None:
        self.rpc_call(
            AuthRevoker.call_auth,
            client_settings.token,
            client_settings.name,
        )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.revoker.authed:
            self.on_manager_session_lost(self.close_reason)


