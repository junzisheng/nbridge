from typing import Optional, List
from asyncio import Future, CancelledError
import uuid

from loguru import logger

from constants import CloseReason
from revoker import AuthRevoker, PingPongRevoker, PingPong
from protocols import BaseProtocol
from manager.client import ClientRevoker
from registry import Registry
from config.settings import server_settings


class ManagerRevoker(AuthRevoker):
    TIMEOUT = 3

    def get_token(self, client_name: str) -> Optional[str]:
        client_config = server_settings.client_map.get(client_name)
        return client_config.token if client_config else None


class ManagerServer(BaseProtocol, PingPong):
    revoker_bases = (ManagerRevoker, PingPongRevoker)
    ping_interval = server_settings.heart_check_interval

    def __init__(self,
                 *,
                 manager_registry: Registry,
                 on_session_made: callable,
                 on_session_lost: callable,
                 proxy_ports=List[int]) -> None:
        super(ManagerServer, self).__init__()
        self.manager_registry = manager_registry
        self.on_session_made = on_session_made
        self.on_session_lost = on_session_lost
        self.client_name: str = ''
        self.proxy_ports = proxy_ports
        self.session_created = False

    def on_auth_success(self, client_name: str) -> None:
        if self.manager_registry.is_registered(client_name):
            self.rpc_call(
                ClientRevoker.call_set_close_reason,
                CloseReason.MANAGER_ALREADY_CONNECTED,
            )
            self.transport.close()
            logger.info(f'Manager Client -【{client_name}】Already Connected - Closed')
        else:
            logger.info(f'Manager Client -【{client_name}】Session Created Success')
            self.session_created = True
            temp_token = str(uuid.uuid4())  # 随机生成token
            self.client_name = client_name
            # 这里需要等到worker收到消息更新token之后才能给客户端通知，
            # 不然可能出现客户端先收到消息，进行proxy client连接，但是proxy server还未接收到token
            process_notify_waiter = self.aexit_context.create_future()

            @process_notify_waiter.add_done_callback
            def callback(f: Future):
                try:
                    f.result()
                except CancelledError:
                    pass
                else:
                    self.rpc_call(
                        ClientRevoker.call_session_created,
                        token=temp_token,
                        proxy_ports=self.proxy_ports,
                        proxy_size=server_settings.proxy_pool_size
                    )
                    self.ping()

            self.on_session_made(
                self,
                temp_token,
                process_notify_waiter,
            )

    def on_auth_fail(self):
        self.rpc_call(
            ClientRevoker.call_set_close_reason,
            CloseReason.AUTH_FAIL
        )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.session_created:
            logger.info(f'Manager 客户端【{self.client_name}】断开连接')
            self.manager_registry.unregister(self.client_name)
            self.on_session_lost(self)
