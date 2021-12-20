from typing import Optional, Any, Callable
import asyncio
from functools import partial
from asyncio import Future, Task, CancelledError

from loguru import logger

from protocols import BaseProtocol, ReConnector
from common_bases import Forwarder
from invoker import AuthInvoker, PingPongInvoker, PingPong
from config.settings import ClientSettings


class LocalProtocol(BaseProtocol, Forwarder):
    def __init__(self, on_connection_made: callable):
        super(LocalProtocol, self).__init__()
        self._on_connection_made = on_connection_made

    def on_connection_made(self) -> None:
        self._on_connection_made(self)

    def data_received(self, data: bytes) -> None:
        self.forward(data)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.close_forwarder()

    def write(self, data: bytes) -> None:
        self.transport.write(data)

    def on_forwarder_close(self) -> None:
        self.transport.close()


TypeProxyClientCallback = Callable[['ProxyClient'], None]


class ProxyClient(BaseProtocol, Forwarder, PingPong):
    invoker_bases = (PingPongInvoker, )

    def __init__(
        self,
        client_settings: ClientSettings,
        on_proxy_session_made: TypeProxyClientCallback,
        on_lost: TypeProxyClientCallback,
        epoch: int
     ) -> None:
        super().__init__()
        self.epoch = epoch
        self.on_proxy_session_made = on_proxy_session_made
        self.on_lost = on_lost
        self.client_settings = client_settings
        self._init()

    def _init(self) -> None:
        self.client_fin = self.server_fin = False
        self._local_task = None

    def on_connection_made(self) -> None:
        self.remote_call(
            AuthInvoker.call_auth,
            self.client_settings.token,
            self.client_settings.name,
            self.epoch
        )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.on_lost(self)

    def rpc_local_pair(self, host: str, port: int) -> None:
        assert self._local_task is None
        self.reset_forwarder()

        def on_connection_made(local: LocalProtocol) -> None:
            print(id(self), self.forwarder_status)
            local.reset_forwarder()
            self.set_forwarder(local)
            local.set_forwarder(self)

        self._local_task = self.aexit_context.create_task(
            self._loop.create_connection(
                partial(LocalProtocol, on_connection_made),
                host=host,
                port=port
            )
        )

        @self._local_task.add_done_callback
        def _(f: Future) -> None:
            try:
                f.result()
            except CancelledError:
                pass
            except RuntimeError as e:
                logger.exception(e)
                self.trigger_client_fin()
            except Exception as e:
                self.trigger_client_fin()
            finally:
                self._local_task = None

    def write(self, data: bytes) -> None:
        assert self.forwarder_status == 1
        from proxy.server import ProxyServer
        self.remote_call(
            ProxyServer.rpc_forward,
            data
        )

    def may_both_fin(self):
        if self.client_fin and self.server_fin:
            self._init()

    def trigger_client_fin(self) -> None:
        from proxy.server import ProxyServer
        assert not self.client_fin
        self.close_forwarder()
        self.client_fin = True
        self.remote_call(ProxyServer.rpc_client_fin)
        self.may_both_fin()

    on_forwarder_close = trigger_client_fin

    def rpc_server_fin(self) -> None:
        assert not self.server_fin
        self.server_fin = True
        if not self.client_fin:
            if self._local_task:
                self._local_task.cancel()
            self.trigger_client_fin()
        else:
            self.may_both_fin()

    def rpc_auth_success(self, *args, **kwargs) -> None:
        self.on_proxy_session_made(self)

    def rpc_forward(self, data: bytes) -> None:
        self.forward(data)


class ProxyConnector(ReConnector):
    def log_fail(self, reason: Exception) -> None:
        logger.info(f'Proxy Server Connect Fail - Retry {self.retries} times')

