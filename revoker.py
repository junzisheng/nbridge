from typing import Callable, Optional, Any, Type, Tuple, TYPE_CHECKING, TypeVar, List
import asyncio
from typing import TYPE_CHECKING
from functools import wraps

from loguru import logger

from constants import CloseReason
from utils import ignore
if TYPE_CHECKING:
    from protocols import BaseProtocol


class Revoker(object):
    def __init__(self, protocol: 'BaseProtocol'):
        self.protocol = protocol

    def call(self, m: str, *args, **kwargs) -> None:
        try:
            getattr(self, m)(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            self.protocol.transport.close()

    @staticmethod
    def call_log(msg: str) -> None:
        logger.info(msg)

    def call_set_close_reason(self, reason) -> None:
        self.protocol.set_close_reason(reason)

    def on_protocol_close(self) -> None:
        pass


class AuthRevoker(Revoker):
    TOKEN = ""
    authed = False

    def __init__(self, protocol: 'BaseProtocol'):
        super().__init__(protocol)
        self.timeout_task = asyncio.get_event_loop().call_later(
            self.TIMEOUT, self._on_timeout
        )
        self._auth_success = getattr(protocol, 'on_auth_success', ignore)
        self._auth_fail = getattr(protocol, 'on_auth_fail', ignore)
        self.hook()

    def hook(self):
        _connection_lost = self.protocol.connection_lost

        def __hook_connection_lost(exc: Optional[Exception]) -> None:
            self.timeout_task.cancel()
            _connection_lost(exc)
        wraps(_connection_lost)(__hook_connection_lost)
        self.protocol.connection_lost = __hook_connection_lost

    def call(self, m: str, *args, **kwargs) -> Any:
        if not self.authed and m != 'call_auth':
            self.protocol.transport.close()
        else:
            super().call(m, *args, **kwargs)

    def get_token(self, host_name: str) -> Optional[str]:
        raise NotImplementedError

    def _on_timeout(self) -> None:
        self._auth_fail()
        self.protocol.transport.close()

    def call_auth(self, token: bytes, client_name: str, *args, **kwargs) -> None:
        from protocols import BaseProtocol
        if self.authed:
            return
        self.timeout_task.cancel()
        _token = self.get_token(client_name)
        if not _token or _token != token:
            self._auth_fail()
            self.protocol.remote_call(
                BaseProtocol.rpc_auth_fail
            )
            self.protocol.transport.close()
            return
        self.authed = True
        self._auth_success(client_name, *args, **kwargs)
        # self.protocol.remote_call(
        #     BaseProtocol.rpc_auth_success
        # )


class PingPongRevoker(Revoker):
    def call_ping(self) -> None:
        self.protocol.remote_call(
            self.call_pong
        )

    def call_pong(self) -> None:
        self.protocol.receive_pong()


class PingPong(object):
    last_pong = True
    ping_interval = 3

    def ping(self):
        from protocols import BaseProtocol
        if not self.last_pong:
            self.remote_call(
                BaseProtocol.rpc_set_close_reason,
                CloseReason.PING_TIMEOUT,
            )
            self.transport.close()
            return
        self.last_pong = False
        self.remote_call(
            PingPongRevoker.call_ping,
        )
        self.aexit_context.call_later(
            self.ping_interval,
            self.ping
        )

    def receive_pong(self) -> None:
        self.last_pong = True


TypeRevoker = TypeVar('Revoker', Revoker, AuthRevoker)

