from typing import Optional
import asyncio
from asyncio import transports

import pytest

from protocols import BaseProtocol, AuthInvoker


@pytest.mark.skip
@pytest.mark.asyncio
async def test_base(event_loop):
    port = 81
    msg = b''
    test_msg = b'test'
    e = asyncio.Event()

    class ServerBaseProtocol(BaseProtocol):
        def on_message(self, data: bytes) -> None:
            assert data == test_msg

    await event_loop.create_server(
        ServerBaseProtocol,
        host='0.0.0.0',
        port=port
    )

    class ClientProtocol(asyncio.Protocol):
        def connection_made(self, transport: transports.Transport) -> None:
            transport.write(test_msg + b'END')
    await event_loop.create_connection(
        ClientProtocol,
        '127.0.0.1',
        port
    )
    await e.wait()
    assert msg == b'test'


@pytest.mark.asyncio
async def test_auth(event_loop):
    port = 82
    await event_loop.create_server(
        type('AuthProtocol', (BaseProtocol,), {'invoker_class': AuthInvoker}),
        host='0.0.0.0',
        port=port
    )
    close_event = asyncio.Event()

    class ClientProtocol(BaseProtocol):
        def connection_made(self, transport: transports.Transport) -> None:
            super().connection_made(transport)
            self.remote_call(AuthInvoker.call_auth, 'bb')

        def connection_lost(self, exc: Optional[Exception]) -> None:
            close_event.set()

    await event_loop.create_connection(
        ClientProtocol,
        '127.0.0.1',
        port
    )










