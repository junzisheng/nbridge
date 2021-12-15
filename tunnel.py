from typing import Union, Optional, TYPE_CHECKING

from state import State

if TYPE_CHECKING:
    from protocols import BaseProtocol


class TunnelPair(object):
    def __init__(self, owner: 'BaseProtocol') -> None:
        self.owner = owner
        self.tunnel: Optional['TunnelPair'] = None

    @property
    def registered(self):
        return bool(self.tunnel)

    def register_tunnel(self, tunnel: 'TunnelPair') -> None:
        assert self.tunnel is None
        self.tunnel = tunnel

    def unregister_tunnel(self) -> None:
        if self.tunnel:
            self.tunnel._unregistered_tunnel()
            self.tunnel = None

    def _unregistered_tunnel(self) -> None:
        if self.tunnel:
            self.tunnel = None

    def forward(self, data: bytes) -> None:
        if self.tunnel:
            self.tunnel.owner.transport.write(data)


class ProxyServerTunnelPair(TunnelPair):
    def register_tunnel(self, tunnel: 'ProxyServerTunnelPair', end_point: tuple) -> None:
        assert self.owner.state.st == State.WORK_PREPARE
        from proxy.client import ProxyRevoker
        super(ProxyServerTunnelPair, self).register_tunnel(tunnel)
        self.owner.state.to(State.WORK)
        self.owner.remote_call(
            ProxyRevoker.call_new_session,
            end_point,
        )

    def unregister_tunnel(self, state_change: bool = True) -> None:
        # if state_change and self.registered:
        self.owner.state.to(State.IDLE)  # todo
        super(ProxyServerTunnelPair, self).unregister_tunnel()

    def _unregistered_tunnel(self) -> None:
        if self.registered:
            from proxy.client import ProxyRevoker
            self.owner.state.to(State.WAIT_CLIENT_READY)
            self.owner.remote_call(
                ProxyRevoker.call_disconnect_session

            )
        super(ProxyServerTunnelPair, self)._unregistered_tunnel()


class ProxyClientTunnelPair(TunnelPair):
    def unregister_tunnel(self) -> None:
        from proxy.server import ServerRevoker
        if self.registered:
            self.owner.remote_call(
                ServerRevoker.call_client_ready
            )
        super(ProxyClientTunnelPair, self).unregister_tunnel()

    def _unregistered_tunnel(self) -> None:
        from proxy.server import ServerRevoker
        if self.tunnel:
            self.owner.remote_call(
                ServerRevoker.call_session_disconnect
            )
        super(ProxyClientTunnelPair, self)._unregistered_tunnel()


class LocalTunnelPair(TunnelPair):
    def _unregistered_tunnel(self) -> None:
        if self.registered:
            self.owner.transport.close()
        super(LocalTunnelPair, self)._unregistered_tunnel()

    def forward(self, data: bytes) -> None:
        from proxy.server import ServerRevoker
        if self.registered:
            self.tunnel.owner.remote_call(
                ServerRevoker.call_forward,
                data
            )


class PublicTunnelPair(TunnelPair):
    def forward(self, data: bytes) -> None:
        if self.registered:
            from proxy.client import ProxyRevoker
            self.tunnel.owner.remote_call(
                ProxyRevoker.call_forward,
                data
            )

    def _unregistered_tunnel(self) -> None:
        if self.registered:
            self.owner.transport.write_eof()
            self.owner.transport.close()
        super(PublicTunnelPair, self)._unregistered_tunnel()
