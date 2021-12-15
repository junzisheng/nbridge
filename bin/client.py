from typing import List, Optional, Iterable, Dict, Union
from collections import defaultdict
import asyncio
from asyncio import Future
from multiprocessing import Process, Pipe
from functools import partial, cached_property

from loguru import logger

from common_bases import Bin
from messager import Event, Message, ProcessPipeMessageKeeper, broadcast_message
from constants import CloseReason
from manager.client import ClientProtocol, ClientConnector
from worker.bases import run_worker, ClientWorkerStruct
from config.settings import client_settings
from worker.client import ClientWorker
from utils import loop_travel


class Client(Bin):
    def __init__(self) -> None:
        super(Client, self).__init__()
        self.workers: Dict[int, ClientWorkerStruct] = {}
        self.client: Optional[ClientProtocol] = None

    def start(self) -> None:
        self.run_worker()
        self.run_client()

    @cached_property
    def message_keepers(self):
        return [p.message_keeper for p in self.workers.values()]

    def run_worker(self) -> None:
        for idx in range(client_settings.workers):
            parent_input_channel, parent_output_channel = Pipe()
            child_input_channel, child_output_channel = Pipe()

            message_keeper = ProcessPipeMessageKeeper(self, parent_input_channel, child_output_channel)
            pro = Process(
                target=run_worker,
                args=(ClientWorker, ProcessPipeMessageKeeper, child_input_channel, parent_output_channel)
            )
            pro.daemon = True
            pro.start()
            self.workers[pro.pid] = ClientWorkerStruct(
                process=pro,
                message_keeper=message_keeper,
            )
            message_keeper.listen()

    def run_client(self) -> None:
        def on_manager_session_made(client: ClientProtocol) -> None:
            self.client = client
            broadcast_message(
                self.message_keepers,
                Event.MANAGER_SESSION_MADE
            )

        def on_manager_session_lost(reason: int) -> None:
            """manager 断开连接"""
            self.client = None
            for w in self.workers.values():
                w.proxy_state = defaultdict(lambda: 0)
            broadcast_message(
                self.message_keepers,
                Event.MANAGER_SESSION_LOST
            )
            # todo waiter message wait all process clean
            if self._stop:
                return
            if reason == CloseReason.UN_EXPECTED:
                logger.info(f'【{client_settings.name}】disconnected unexpected retry')
                self.run_client()
            if reason == CloseReason.MANAGER_ALREADY_CONNECTED:
                logger.info(f'【{client_settings.name}】already connected, close ~~')
                self.closer.call_close()
            elif reason == CloseReason.AUTH_FAIL:
                logger.info(f'【{client_settings.name}】auth fail, close ~~')
                self.closer.call_close()
            elif reason == CloseReason.PING_TIMEOUT:
                logger.info(f'【{client_settings.name}】ping timeout, retry')
                self.run_client()
            elif reason == CloseReason.SERVER_CLOSE:
                logger.info(f'【{client_settings.name} Server Close, Retry')
                self.run_client()

        connector = ClientConnector(
            partial(
                ClientProtocol,
                on_manager_session_made=on_manager_session_made,
                on_manager_session_lost=on_manager_session_lost,
                apply_new_proxy=self.apply_new_proxy
            ),
            host=client_settings.server_host,
            port=client_settings.server_port,
        )
        connector.connect()
        f = self.aexit.create_future()

        @f.add_done_callback
        def on_close(r: Future) -> None:
            connector.abort()

        connector.set_waiter(f)

    def apply_new_proxy(self, epoch: int, port: int) -> None:
        all_workers = list(self.workers.values())
        all_workers.sort(key=lambda w: [w.proxy_state[port], sum(w.proxy_state.values())])
        worker = all_workers[0]
        worker.proxy_state[port] += 1
        worker.put(
            Message(
                Event.PROXY_APPLY,
                epoch,
                port
            )
        )

    def on_message_proxy_lost(self, pid: int, port: int) -> None:
        if self.client:
            self.workers[pid].proxy_state[port] -= 1

    async def do_handle_stop(self) -> None:
        if self.client:
            self.client.transport.close()
        logger.info('Manager Server Close')
        for worker in self.workers.values():
            message_keeper = worker.message_keeper
            pro = worker.process
            message_keeper.send(Message(Event.SERVER_CLOSE))
            message_keeper.stop()
            pro.join()
            message_keeper.close()
            logger.info(f'Process【{pro.pid}】Closed')
        # for all task cancelled
        await asyncio.sleep(0)


if __name__ == '__main__':
    Client().run()
