from typing import Optional, List, Dict
import sys
import json

from loguru import logger

from prettytable import PrettyTable

from protocols import BaseProtocol, ReConnector
from revoker import Revoker
from state import State

Column = ["PID", State.IDLE, State.WORK, State.WAIT_CLIENT_READY, State.WAIT_AUTH, State.DISCONNECT]


class MonitorRevoker(Revoker):

    @staticmethod
    def call_notify_state(state: str) -> None:
        print(state)
        # state: Dict[str, Dict[int, int]] = json.loads(state)
        # sys.stdout.flush()
        # sys.stdout.write('*' * 50)
        # l = []
        # for host_name, pid_state in state.items():
        #     pid_items = list(pid_state.items())
        #     pid_items.sort(key=lambda x: x[0])
        #     l.append([host_name, pid_items])
        # l.sort(key=lambda x: x[0])
        #
        # table = PrettyTable(Column)
        # for host_name, pid_items in l:
        #     print('\r\n')
        #     print(f'---------{host_name}---------')
        #
        #     for pid, idle_count in pid_items:
        #         row = [pid, idle_count]
        #         table.add_row(row)
        # sys.stdout.write(str(table))


class MonitorServer(BaseProtocol):
    protocols_list: List['MonitorServer'] = []

    def __init__(self, on_session_made: callable):
        super(MonitorServer, self).__init__()
        self.on_session_made = on_session_made

    def on_connection_made(self) -> None:
        self.protocols_list.append(self)
        self.on_session_made(self)

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.protocols_list.remove(self)

    def notify_state(self, state: dict) -> None:
        self.rpc_call(MonitorRevoker.call_notify_state, state)

    @classmethod
    def notify_all_state(cls, state: dict) -> None:
        for pro in cls.protocols_list:
            pro.notify_state(state)


class MonitorClient(BaseProtocol):
    revoker_bases = (MonitorRevoker,)

    def on_connection_made(self) -> None:
        logger.info('Monitor Connect Success')


class MonitorConnector(ReConnector):
    mode = "always"


