from typing import Optional, Callable
import h11

from loguru import logger

from common_bases import Forwarder
from protocols import BaseProtocol

PublicCallbackType = Callable[[BaseProtocol], None]


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


class HttpPublicProtocol(BaseProtocol, Forwarder):
    def __init__(self, request_event_received: PublicCallbackType, on_connection_lost: PublicCallbackType) -> None:
        super(HttpPublicProtocol, self).__init__()
        self.request_event_received = request_event_received
        self._on_connection_lost = on_connection_lost
        self.conn = h11.Connection(h11.SERVER)
        self.domain: Optional[str] = None

    def on_connection_made(self) -> None:
        self.reset_forwarder()

    def data_received(self, data: bytes) -> None:
        self.forward(data)
        if self.domain is None:
            self.conn.receive_data(data)
            self.handle_events()

    def handle_events(self) -> None:
        while True:
            try:
                event = self.conn.next_event()
            except h11.RemoteProtocolError as exc:
                self.transport.close()
                return
            event_type = type(event)
            if event is h11.NEED_DATA:
                break
            if event_type is h11.Request:
                for key, value in event.headers:
                    if key == b'host':
                        self.domain = value.decode('ascii')
                        self.request_event_received(self)
                        return
            else:
                raise

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.conn.our_state != h11.ERROR:
            event = h11.ConnectionClosed()
            try:
                self.conn.send(event)
            except h11.LocalProtocolError:
                pass
        self._on_connection_lost(self)
        self.close_forwarder()

    def write(self, data: bytes) -> None:
        assert self.forwarder_status
        self.transport.write(data)

    def on_forwarder_close(self) -> None:
        self.transport.close()

from tinydb import TinyDB