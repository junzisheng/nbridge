from typing import Optional, Tuple
import pickle
import random
import socket
import asyncio
from asyncio import CancelledError

import pytest

from protocols import BaseProtocol, Parser, Connector, ReConnector


class TestParser(object):

    @pytest.mark.parametrize('extra', [b'1', b'2', b''])
    def test_parser(self, extra):
        parser = Parser()
        expected = []
        for i in range(random.randint(2, 10)):
            expected.append((f'method{i}', (f'args{i}',), {f'kwargs{i}': i}))
        packet = b''
        for p in expected:
            packet += parser.loads(p[0], *p[1], **p[2])
        packet += extra
        result = []
        parser.dumps(packet, lambda d: result.append(pickle.loads(d)))
        assert expected == result
        assert parser.unprocessed == extra


class TestBaseProtocol(object):

    @staticmethod
    async def get_pair_protocol() -> Tuple[BaseProtocol, BaseProtocol]:
        loop = asyncio.get_event_loop()
        server, client = socket.socketpair()
        _, server_p = await loop.create_connection(
            BaseProtocol,
            sock=server
        )
        _, client_p = await loop.create_connection(
            BaseProtocol,
            sock=client
        )
        return server_p, client_p

    @pytest.mark.asyncio
    async def test_close(self, event_loop):
        server, client = await self.get_pair_protocol()
        assert client.close_reason == 0
        server.close(-1)
        await asyncio.sleep(0.01)
        assert server.transport.is_closing()
        # await asyncio.sleep(0)
        assert client.close_reason == -1

    @pytest.mark.asyncio
    async def test_multi_call(self, event_loop):
        server, client = await self.get_pair_protocol()
        test_results = []

        def rpc_test(*args, **kwargs):
            test_results.append((args, kwargs))
        client.rpc_test = rpc_test

        server.remote_multi_call([
            (client.rpc_test, (1,), {'a': 2}),
            (client.rpc_test, (2,), {'a': 3}),
        ])
        await asyncio.sleep(0.01)
        assert test_results == [((1,), {'a': 2}), ((2,), {'a': 3})]


@pytest.mark.asyncio
@pytest.mark.parametrize('abort', [False, True])
async def test_connector(event_loop, abort):
    server = await event_loop.create_server(BaseProtocol, host='127.0.0.1', port=0)
    port = server.sockets[0].getsockname()[1]
    connector = Connector(BaseProtocol, host='127.0.0.1', port=port)
    task = connector.connect()
    if abort:
        connector.abort()
    try:
        _, client = await task
        assert isinstance(client, BaseProtocol)
    except BaseException as e:
        assert isinstance(e, CancelledError)


# @pytest.mark.asyncio
# @pytest.mark.parametrize('abort', [False, True])
# async def test_reconnector(event_loop, abort):
#     connector = ReConnector(BaseProtocol, host='127.0.0.1', port=123)
#     connector.connect()
#     connector.delay = 0.1
#     await asyncio.sleep(connector.delay*5)
#     assert connector.retries > 0

