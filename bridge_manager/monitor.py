import os
from typing import Optional, List, Dict
import sys

from prettytable import PrettyTable

from protocols import BaseProtocol
from revoker import Revoker
from state import State

Column = ["PID", State.IDLE, State.WORK, State.WAIT_CLIENT_READY, State.WAIT_AUTH, State.DISCONNECT]


class MonitorRevoker(Revoker):
    state: Dict[int, Dict[str, int]] = {}

    def call_report(self, state) -> None:
        self.state.update(state)
        self.print()

    def print(self):
        table = PrettyTable(Column)
        for pid, state in self.state.items():
            l = [pid]
            for c in Column[1:]:
                l.append(state.get(c, 0))
            table.add_row(l)
        sys.stdout.flush()
        sys.stdout.write('\r')
        sys.stdout.write('\r')
        sys.stdout.write('*'*50)
        sys.stdout.write(str(table))


class MonitorServer(BaseProtocol):
    protocols_list: List['MonitorServer'] = []

    def on_connection_made(self) -> None:
        self.protocols_list.append(self)

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.protocols_list.remove(self)

    @classmethod
    def broadcast(cls, state: Dict[int, Dict[str, int]]):
        for protocol in cls.protocols_list:
            protocol.rpc_call(
                MonitorRevoker.call_report,
                state
            )


class MonitorClient(BaseProtocol):
    revoker_bases = (MonitorRevoker,)
