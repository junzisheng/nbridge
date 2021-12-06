from typing import Optional
import socket
import asyncio
from asyncio import constants
import errno

from loguru import logger

from protocols import BaseProtocol
from tunnel import PublicTunnelPair


class PublicProtocol(BaseProtocol):
    def __init__(self, end_point: tuple, require_proxy: callable):
        super(PublicProtocol, self).__init__()
        self.tunnel = PublicTunnelPair(self)
        self.require_proxy = require_proxy
        self.end_point = end_point

    def on_connection_made(self) -> None:
        from bridge_proxy.server import ProxyServer
        proxy: ProxyServer = self.require_proxy()
        if not proxy:
            self.transport.close()
        else:
            self.tunnel.register_tunnel(proxy.tunnel)
            proxy.tunnel.register_tunnel(self.tunnel, self.end_point)
            logger.debug('public server connected')

    def data_received(self, data: bytes) -> None:
        self.tunnel.forward(data)

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        logger.debug('public server disconnected')
        self.tunnel.unregister_tunnel()


def start_public_server(sa, post: callable) -> socket.socket:
    backlog = 100
    loop = asyncio.get_event_loop()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)
    server.bind(sa)
    server.setblocking(False)
    server.listen(backlog)

    def _accept_connection():
        for i in range(backlog):
            try:
                conn, addr = server.accept()
                conn.set_inheritable(True)
                post(conn)
            except (BlockingIOError, InterruptedError, ConnectionAbortedError):
                return None
            except OSError as exc:
                if exc.errno in (errno.EMFILE, errno.ENFILE,
                                 errno.ENOBUFS, errno.ENOMEM):
                    loop.remove_reader(server.fileno())
                    loop.call_later(
                        constants.ACCEPT_RETRY_DELAY,
                        _start_serving
                    )

    def _start_serving():
        loop.add_reader(server.fileno(), _accept_connection)

    _start_serving()
    return server









