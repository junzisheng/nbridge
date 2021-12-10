from typing import Optional

from protocols import BaseProtocol
from revoker import AuthRevoker, PingPongRevoker, PingPong
from state import State
from tunnel import ProxyServerTunnelPair
from config.settings import server_settings


class ServerRevoker(AuthRevoker):
    TIMEOUT = 3
    def get_token(self, host_name: str) -> str:
        return self.protocol.get_token(host_name)

    def call_session_disconnect(self) -> None:
        self.protocol.tunnel.unregister_tunnel()

    def call_client_ready(self) -> None:
       self.protocol.state.to(State.IDLE)

    def call_forward(self, body: bytes) -> None:
        self.protocol.tunnel.forward(body)


class ProxyState(State):
    def __init__(self, protocol: 'ProxyServer') -> None:
        super(ProxyState, self).__init__()
        self.protocol = protocol

    def to(self, st) -> None:
        pre_state = self.st
        super(ProxyState, self).to(st)
        if pre_state != self.st:
            self.protocol.report_state(pre_state, st, self.protocol)


class ProxyServer(BaseProtocol, PingPong):
    revoker_bases = (ServerRevoker, PingPongRevoker)
    ping_interval = server_settings.heart_check_interval

    def __init__(self, get_token: callable, report_state: callable) -> None:
        super().__init__()
        self.get_token = get_token
        self.report_state = report_state
        self.host_name = ''
        self.state = ProxyState(self)
        self.tunnel = ProxyServerTunnelPair(self)

    def on_auth_success(self, host_name: str) -> None:
        from bridge_proxy.client import ProxyRevoker
        self.rpc_call(
            ProxyRevoker.call_auth_success
        )
        self.host_name = host_name
        self.state.to(State.IDLE)
        self.ping()

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.revoker.authed:
            self.tunnel.unregister_tunnel(False)
            self.state.to(State.DISCONNECT)

    def on_auth_fail(self):
        pass




