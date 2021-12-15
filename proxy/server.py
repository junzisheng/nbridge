from typing import Optional, Callable, Dict, TYPE_CHECKING
from socket import socket

from protocols import BaseProtocol
from constants import CloseReason
from common_bases import Forwarder
from revoker import AuthRevoker, PingPongRevoker, PingPong
from config.settings import server_settings
if TYPE_CHECKING:
    from public.protocol import PublicProtocol
    from worker.bases import ClientStruct


class ServerRevoker(AuthRevoker):
    TIMEOUT = 3

    def get_token(self, client_name: str) -> Optional[str]:
        client_config = server_settings.client_map.get(client_name)
        return client_config.token if client_config else None


ProxyServerCallbackType = Callable[['ProxyServer'], None]


class ProxyServer(BaseProtocol, PingPong, Forwarder):
    revoker_bases = (ServerRevoker, PingPongRevoker)
    ping_interval = server_settings.heart_check_interval

    client_name = ''
    session_made = False

    def __init__(
        self, client_info: Dict[str, 'ClientStruct'],
        on_session_made: ProxyServerCallbackType,
        on_session_lost: ProxyServerCallbackType,
        on_task_done: ProxyServerCallbackType,
    ) -> None:
        super().__init__()
        self.client_info = client_info
        self.on_session_made = on_session_made
        self.on_session_lost = on_session_lost
        self.on_task_done = on_task_done
        self._init()

    def _init(self) -> None:
        self.client_fin = self.server_fin = False

    def on_auth_success(self, client_name: str, epoch: int) -> None:
        from proxy.client import ProxyClient
        client = self.client_info.get(client_name)
        if not client:
            self.remote_multi_call(
                [
                    (ProxyClient.rpc_auth_fail, (), {}),
                    (ProxyClient.rpc_log, f'Client Name: {client_name} Unknown', {})
                ]
            )
            self.close(CloseReason.CLIENT_NAME_UNKNOWN)
        elif client.epoch != epoch:
            self.remote_multi_call(
                [
                    (ProxyClient.rpc_auth_fail, (), {}),
                    (ProxyClient.rpc_log, (f'Server epoch: {client.epoch} - Client epoch: {epoch}; Epoch Error', ), {})
                ]
            )
            self.close(CloseReason.CLINET_EPOCH_EXPIRED)
        else:
            self.remote_call(
                ProxyClient.rpc_auth_success
            )
            self.session_made = True
            self.client_name = client_name
            self.on_session_made(self)
            self.ping()

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.session_made:
            self.on_session_lost(self)
            self.close_forwarder()

    def set_forwarder(self, forwarder: 'PublicProtocol') -> None:
        super(ProxyServer, self).set_forwarder(forwarder)
        from proxy.client import ProxyClient
        sock: socket = forwarder.transport.get_extra_info('socket')
        _, port = sock.getsockname()
        public_config = server_settings.public_port_map[port]
        self.remote_call(
            ProxyClient.rpc_local_pair,
            public_config.local_host,
            public_config.local_port
        )

    def write(self, data: bytes) -> None:
        assert self.forwarder_status == 1
        from proxy.client import ProxyClient
        self.remote_call(
            ProxyClient.rpc_forward,
            data
        )

    def may_both_fin(self) -> None:
        if self.client_fin and self.server_fin:
            self._init()
            self.on_task_done(self)

    def trigger_server_fin(self) -> None:
        assert not self.server_fin
        self.close_forwarder()
        self.server_fin = True
        from proxy.client import ProxyClient
        self.remote_call(ProxyClient.rpc_server_fin)
        self.may_both_fin()

    on_forwarder_close = trigger_server_fin

    def rpc_forward(self, data: bytes) -> None:
        self.forward(data)

    def rpc_client_fin(self) -> None:
        assert not self.client_fin
        self.client_fin = True
        if not self.server_fin:
            self.trigger_server_fin()
        else:
            self.may_both_fin()

