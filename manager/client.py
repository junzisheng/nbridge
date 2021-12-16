from typing import List, Optional, Callable

from loguru import logger

from invoker import AuthInvoker, PingPongInvoker, PingPong
from protocols import BaseProtocol, ReConnector
from config.settings import ClientSettings


class ClientProtocol(BaseProtocol, PingPong):
    invoker_bases = (PingPongInvoker, )
    server_worker_info: Optional[List[str]] = None
    session_made = False
    epoch = None

    def __init__(
            self, *, client_settings: ClientSettings , on_manager_session_made: callable, on_manager_session_lost: callable,
            apply_new_proxy: Callable[[int, int], None],
    ) -> None:
        super().__init__()
        self.on_manager_session_made = on_manager_session_made
        self.on_manager_session_lost = on_manager_session_lost
        self.apply_new_proxy = apply_new_proxy
        self.client_settings = client_settings

    def on_connection_made(self) -> None:
        self.remote_call(
            AuthInvoker.call_auth,
            self.client_settings.token,
            self.client_settings.name,
        )
        self.on_manager_session_made(self)
        
    def rpc_session_made(self, server_worker_info: List[str]) -> None:
        self.session_made = True
        self.server_worker_info = server_worker_info

    def rpc_auth_success(self, workers: List[str], epoch: int) -> None:
        self.session_made = True
        self.server_worker_info = workers
        self.epoch = epoch

    def rpc_create_proxy(self, port: int, size: int) -> None:
        for i in range(size):
            self.apply_new_proxy(self.epoch, port)

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.on_manager_session_lost(self.close_reason)


class ClientConnector(ReConnector):
    def log_fail(self, reason: Exception) -> None:
        logger.info(f'Manager Connect Fail - Retry {self.retries} - {reason.__class__}')


