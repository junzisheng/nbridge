from typing import Optional, Dict
from asyncio import Future, TimerHandle, CancelledError
import uuid

from loguru import logger

from constants import CloseReason
from revoker import AuthRevoker, Revoker, PingPongRevoker, PingPong
from protocols import BaseProtocol
from bridge_manager.client import ClientRevoker
from registry import Registry
from settings import settings
from bridge_manager.worker import WorkerStruct


class ManagerRevoker(Revoker):
    def __init__(self, protocol: 'ManagerServer') -> None:  # for type
        self.protocol = protocol


class ManagerServer(BaseProtocol, PingPong):
    revoker_bases = (AuthRevoker, ManagerRevoker, PingPongRevoker)
    registry = Registry()
    ping_interval = 1

    def __init__(self, session_created_waiter: Future, session_disconnect_waiter: Future, workers: Dict[int, WorkerStruct]) -> None:
        super(ManagerServer, self).__init__()
        self.host_name: str = ''
        self.session_created_waiter = session_created_waiter
        self.session_disconnect_waiter = session_disconnect_waiter
        self.last_pong = True
        self.heart_task: Optional[TimerHandle] = None
        self.workers = workers
        self.session_created = False

    def get_revoker_attrs(self) -> Dict:
        return {'TOKEN': settings.token}

    def on_auth_success(self, host_name: str) -> None:
        if self.registry.is_registered(host_name):
            self.rpc_call(
                ClientRevoker.call_set_close_reason,
                CloseReason.MANAGER_ALREADY_CONNECTED,
            )
            self.transport.close()
            logger.info(f'Manager 客户端【{host_name}】重复连接，已关闭')
        else:
            logger.info(f'Manager 客户端【{host_name}】连接成功')
            self.session_created = True
            temp_token = str(uuid.uuid4())  # 随机生成token
            self.host_name = host_name
            self.registry.register(host_name, self)
            # 这里需要等到worker收到消息更新token之后才能给客户端通知，
            # 不然可能出现客户端先收到消息，进行proxy client连接，但是proxy server还未接收到token
            process_receive_waiter = self.aexit_context.create_future()

            @process_receive_waiter.add_done_callback
            def callback(f: Future):
                try:
                    f.result()
                    self.rpc_call(
                        ClientRevoker.call_session_created,
                        token=temp_token,
                        proxy_port_list=[w.port for w in self.workers.values()]
                    )
                except CancelledError:
                    pass
            self.session_created_waiter.set_result((self, temp_token, process_receive_waiter))
            self.ping()

    def on_auth_fail(self):
        self.rpc_call(
            ClientRevoker.call_set_close_reason,
            CloseReason.AUTH_FAIL
        )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        if self.session_created:
            logger.info(f'Manager 客户端【{self.host_name}】断开连接')
            self.registry.unregister(self.host_name)
            self.session_disconnect_waiter.set_result(self)


