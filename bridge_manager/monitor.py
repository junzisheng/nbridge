from typing import Optional, List, Dict
from collections import defaultdict
import sys
from asyncio import Future
import json

from prettytable import PrettyTable

from protocols import BaseProtocol
from bridge_manager.worker import WorkerStruct
from revoker import Revoker
from state import State

Column = ["PID", State.IDLE, State.WORK, State.WAIT_CLIENT_READY, State.WAIT_AUTH, State.DISCONNECT]


class MonitorRevoker(Revoker):
    def call_print(self, state: str) -> None:
        state: Dict[str, Dict[int, int]] = json.loads(state)
        sys.stdout.flush()
        sys.stdout.write('*' * 50)
        l = []
        for host_name, pid_state in state.items():
            pid_items = list(pid_state.items())
            pid_items.sort(key=lambda x: x[0])
            l.append([host_name, pid_items])
        l.sort(key=lambda x: x[0])

        table = PrettyTable(Column)
        for host_name, pid_items in l:
            print('\r\n')
            print(f'---------{host_name}---------')

            for pid, idle_count in pid_items:
                row = [pid, idle_count]
                table.add_row(row)
        sys.stdout.write(str(table))


class MonitorServer(BaseProtocol):
    protocols_list: List['MonitorServer'] = []
    server = None

    @classmethod
    def get_workers(cls):
        return cls.server.workers

    @classmethod
    def build_state(cls) -> Dict:
        state: Dict[str, Dict[int, Dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
        for pid, worker in cls.get_workers().items():
            for host_name, idle_count in worker.proxy_state_shot.items():
                state[host_name][pid] = idle_count
        return state

    def on_connection_made(self) -> None:
        self.protocols_list.append(self)
        state = self.build_state()
        if state:
            self.rpc_call(
                MonitorRevoker.call_print,
                json.dumps(state)
            )

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.protocols_list.remove(self)

    @classmethod
    def broadcast(cls) -> None:
        state = cls.build_state()
        if state:
            for protocol in cls.protocols_list:
                protocol.rpc_call(
                    MonitorRevoker.call_print,
                    json.dumps(state)
                )


class MonitorClient(BaseProtocol):
    revoker_bases = (MonitorRevoker,)

    def __init__(self, disconnect_waiter: Future) -> None:
        super(MonitorClient, self).__init__()
        self.disconnect_waiter = disconnect_waiter

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        self.disconnect_waiter.set_result(None)
