from typing import Optional, Callable

from loguru import logger

from common_bases import Forwarder
from protocols import BaseProtocol

PublicCallbackType = Callable[['PublicProtocol'], None]


class PublicProtocol(BaseProtocol, Forwarder):
    def __init__(self, on_connection_made: PublicCallbackType, on_connection_lost: PublicCallbackType) -> None:
        super(PublicProtocol, self).__init__()
        self._on_connection_made = on_connection_made
        self._on_connection_lost = on_connection_lost

    def on_connection_made(self) -> None:
        self.reset_forwarder()
        self._on_connection_made(self)

    def data_received(self, data: bytes) -> None:
        self.forward(data)

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self._on_connection_lost(self)
        self.close_forwarder()

    def write(self, data: bytes) -> None:
        assert self.forwarder_status
        self.transport.write(data)

    def on_forwarder_close(self) -> None:
        self.transport.close()





