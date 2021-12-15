from typing import Optional, Dict, Callable, List, Tuple
from asyncio import Future

from loguru import logger

from constants import CloseReason
from revoker import AuthRevoker, PingPongRevoker, PingPong
from worker.bases import ServerWorkerStruct
from protocols import BaseProtocol
from manager.client import ClientProtocol
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
    
    client_name: str = ''
    session_created = False

    def __init__(
        self, workers: Dict[int, ServerWorkerStruct], manager_registry: Registry, 
        on_session_made: Callable[['ManagerServer'], Future],
        on_session_lost: Callable[['ManagerServer'], None],
    ) -> None:
        super(ManagerServer, self).__init__()
        self.workers = workers
        self.manager_registry = manager_registry
        self.on_session_made = on_session_made
        self.on_session_lost = on_session_lost

    def on_auth_success(self, client_name: str) -> None:
        if self.manager_registry.is_registered(client_name):
            self.close(CloseReason.MANAGER_ALREADY_CONNECTED)
            logger.info(f'Manager Client -【{client_name}】Already Connected - Closed')
        else:
            logger.info(f'Manager Client -【{client_name}】Session Created Success')
            self.session_created = True
            client_config = server_settings.client_map[client_name]
            self.client_name = client_name
            # 这里需要等到worker收到消息更新token之后才能给客户端通知，
            # 不然可能出现客户端先收到消息，进行proxy client连接，但是proxy server还未接收到token
            self.remote_call(
                ClientProtocol.rpc_auth_success,
                workers=[w.name for w in self.workers.values()],
                epoch=client_config.epoch
            )
            self.ping()
            f = self.on_session_made(self)
            self.aexit_context.monitor_future(f)

            self.remote_call(
                BaseProtocol.rpc_log,
                'Manager Client 登录成功'
            )

            # 需要所有进程接收客户端连接成功后才能发送proxy make的命令
            @f.add_done_callback
            def _(_) -> None:
                worker_list = list(self.workers.values())
                worker_length = len(self.workers)
                s, y = divmod(server_settings.proxy_pool_size, worker_length)
                remote_calls: List = []
                for idx, w in enumerate(worker_list):
                    num = s + (idx < y)
                    remote_calls.extend(
                        [
                            (ClientProtocol.rpc_make_proxy, (w.proxy_port, num), {}),
                            (ClientProtocol.rpc_log, (f'port: {w.proxy_port} - 连接数: {num}',), {})
                        ]
                    )
                self.remote_multi_call(
                    remote_calls
                )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.session_created:
            logger.info(f'Manager 客户端【{self.client_name}】断开连接')
            self.on_session_lost(self)
