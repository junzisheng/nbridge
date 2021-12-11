from typing import Optional, Any
import asyncio
from asyncio import Future, Task, CancelledError

from loguru import logger

from protocols import BaseProtocol, ReConnector
from revoker import AuthRevoker, Revoker, PingPongRevoker
from tunnel import LocalTunnelPair, ProxyClientTunnelPair
from config.settings import client_settings


class LocalProtocol(BaseProtocol):
    def __init__(self):
        super(LocalProtocol, self).__init__()
        self.tunnel = LocalTunnelPair(self)

    def data_received(self, data: bytes) -> None:
        self.tunnel.forward(data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.tunnel.unregister_tunnel()


class ProxyRevoker(Revoker):
    buffer = b''
    _task: Optional[Task] = None

    def call_auth_success(self):
        self.protocol.on_proxy_session_made()

    def call_forward(self, body: bytes) -> None:
        if self.protocol.tunnel.registered:
            self.protocol.tunnel.forward(body)
        else:
            self.buffer += body  # todo flow control

    def call_new_session(self, end_point: tuple) -> None:
        def local_create_result(f: Future) -> None:
            try:
                _, local_protocol = f.result()  # type: Any, LocalProtocol
                local_tunnel = local_protocol.tunnel
                proxy_tunnel = self.protocol.tunnel
                local_tunnel.register_tunnel(proxy_tunnel)
                proxy_tunnel.register_tunnel(local_protocol.tunnel)
                if self.buffer:
                    proxy_tunnel.forward(self.buffer)
                    self.buffer = b''
            except CancelledError:
                pass
            except:
                from proxy.server import ServerRevoker
                self.protocol.rpc_call(
                    ServerRevoker.call_session_disconnect
                )
        host, port = end_point
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(loop.create_connection(
            LocalProtocol,
            host=host,
            port=port
        ))
        self._task.add_done_callback(local_create_result)

    def call_disconnect_session(self):
        if self._task and not self._task.done():
            self._task.cancel()
            # cancel 导致tunnel还为建立， 所以需要这里调用ready
            from proxy.server import ServerRevoker
            self.protocol.rpc_call(
                ServerRevoker.call_client_ready
            )
        else:
            self.protocol.tunnel.unregister_tunnel()

    def on_protocol_close(self) -> None:
        self.buffer = b''
        if self._task and not self._task.cancelled():
            self._task.cancel()


class ProxyClient(BaseProtocol):
    revoker_bases = (ProxyRevoker, PingPongRevoker)

    def __init__(
            self, *, on_proxy_session_made: callable, on_proxy_session_lost, token: str
     ) -> None:
        super().__init__()
        self._on_proxy_session_made = on_proxy_session_made
        self.on_proxy_session_lost = on_proxy_session_lost
        self.token = token
        self.tunnel = ProxyClientTunnelPair(self)
        self.auth_success_waiter = self._loop.create_future()
        self.disconnect_waiter = self._loop.create_future()
        self.session_created = False

    def on_proxy_session_made(self):
        self.session_created = True
        self._on_proxy_session_made(self)

    def on_connection_made(self) -> None:
        self.rpc_call(
            AuthRevoker.call_auth,
            self.token,
            client_settings.name,
        )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.tunnel.unregister_tunnel()
        if self.session_created:
            self.on_proxy_session_lost(self)


class ProxyConnector(ReConnector):
    def log_fail(self, reason: Exception) -> None:
        logger.info(f'Proxy Server Connect Fail - Retry {self.retries} times')

