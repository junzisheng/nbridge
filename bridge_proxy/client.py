from typing import Optional, Any
import asyncio
from asyncio import Future, Task, CancelledError

from loguru import logger

from protocols import BaseProtocol
from revoker import AuthRevoker, Revoker, PingPongRevoker, PingPong
from tunnel import LocalTunnelPair, ProxyClientTunnelPair


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
        self.protocol.auth_success_waiter.set_result(self.protocol)

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
                from bridge_proxy.server import ServerRevoker
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
            from bridge_proxy.server import ServerRevoker
            self.protocol.rpc_call(
                ServerRevoker.call_client_ready
            )
        else:
            self.protocol.tunnel.unregister_tunnel()


class ProxyClient(BaseProtocol, PingPong):
    revoker_bases = (ProxyRevoker, PingPongRevoker)

    def __init__(self, host_name: str, token: str) -> None:
        super().__init__()
        self.host_name = host_name
        self.token = token
        self.tunnel = ProxyClientTunnelPair(self)
        self.auth_success_waiter = self._loop.create_future()
        self.disconnect_waiter = self._loop.create_future()

    def on_connection_made(self) -> None:
        self.rpc_call(
            AuthRevoker.call_auth,
            self.token,
            self.host_name
        )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.tunnel.unregister_tunnel()
        self.revoker.buffer = b''
        if self.auth_success_waiter.done():
            self.disconnect_waiter.set_result(self)

