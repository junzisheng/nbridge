from typing import Optional, Dict, Callable
from asyncio import Future

from loguru import logger
from tinydb import where
from tinydb.operations import set

from constants import CloseReason, ManagerState
from invoker import AuthInvoker, PingPongInvoker, PingPong
from worker.bases import ServerWorkerStruct
from protocols import BaseProtocol
from manager.client import ClientProtocol
from config.settings import server_settings, client_table


class ManagerInvoker(AuthInvoker):
    TIMEOUT = 3

    def get_token(self, client_name: str) -> Optional[str]:
        client = client_table.get(where('client_name') == client_name)
        if client is None:
            return None
        return client['token']


class ManagerServer(BaseProtocol, PingPong):
    invoker_bases = (ManagerInvoker, PingPongInvoker)
    ping_interval = server_settings.heart_check_interval
    
    client_name: str = ''
    session_status = 0

    def __init__(
        self,
        workers: Dict[int, ServerWorkerStruct],
        on_session_made: Callable[[str, Future], None],
        on_session_lost: Callable[['ManagerServer'], None],
    ) -> None:
        super(ManagerServer, self).__init__()
        self.workers = workers
        self.on_session_made = on_session_made
        self.on_session_lost = on_session_lost

    def on_auth_success(self, client_name: str) -> None:
        client = client_table.get(where('client_name') == client_name)
        if client is None:
            self.close(CloseReason.CLIENT_NAME_UNKNOWN)
        elif client['state'] != ManagerState.idle:
            self.close(CloseReason.MANAGER_ALREADY_CONNECTED)
        else:
            logger.info(f'Manager Client -【{client_name}】Session Created Success')
            self.session_status = 1
            self.client_name = client_name
            # 这里需要等到worker收到消息更新token之后才能给客户端通知，
            # 不然可能出现客户端先收到消息，进行proxy client连接，但是proxy server还未接收到token
            self.remote_call(
                ClientProtocol.rpc_auth_success,
                workers=[w.name for w in self.workers.values()],
                epoch=client['epoch']
            )
            self.ping()
            self.remote_call(
                BaseProtocol.rpc_log,
                'Manager Client 登录成功'
            )

            def perform_update(doc: Dict) -> None:
                doc['state'] = ManagerState.session
                doc['client'] = self

            client_table.update(
                perform_update,
                where('client_name') == client_name,
            )
            f = self._loop.create_future()
            self.aexit_context.monitor_future(f)

            # 需要所有进程接收客户端连接成功后才能发送proxy make的命令
            @f.add_done_callback
            def _(_) -> None:
                for w in self.workers.values():
                    self.create_proxy(w.proxy_port, server_settings.proxy_pool_size)
            self.on_session_made(client_name, f)

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.session_status == 1:
            logger.info(f'Manager 客户端【{self.client_name}】断开连接')

            def perform_update(doc: Dict) -> None:
                doc['state'] = ManagerState.closing
                doc['epoch'] += 1
                doc['client'] = None
            client_table.update(
                perform_update,
                where('client_name') == self.client_name,
            )
            self.on_session_lost(self)
        self.session_status = -1

    def create_proxy(self, port: int, num: int) -> None:
        if self.session_status == 1:
            self.remote_multi_call(
                [
                    (ClientProtocol.rpc_create_proxy, (port, num), {}),
                    (ClientProtocol.rpc_log, (f'port: {port} - 连接数: {num}',), {})
                ]
            )
