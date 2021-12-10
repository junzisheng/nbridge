from typing import List, Optional, Iterable
import asyncio
from asyncio import Future
from multiprocessing import Queue
from multiprocessing import Process
from functools import partial

from loguru import logger

from messager import Message, ProcessQueueMessageKeeper, broadcast_message
from constants import CloseReason
from aexit_context import AexitContext
from bridge_manager.client import ClientProtocol
from worker import Event, run_worker
from config.settings import client_settings
from bridge_manager.manager_worker import ClientWorker
from utils import loop_travel


class Client(object):
    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self.message_keepers: [ProcessQueueMessageKeeper] = []
        self.processes: List[Process] = []
        self.iter_message_keeper: Optional[Iterable] = None
        self.aexit_context = AexitContext()

    def run(self) -> None:
        self.run_worker()
        self.run_client()
        self._loop.run_forever()

    def run_client(self, retries=0, delay=0) -> None:
        async def _create_connection():
            if delay > 0:
                await asyncio.sleep(delay)
            return await self._loop.create_connection(
                partial(
                    ClientProtocol,
                    on_manager_session_made=self._on_manager_session_made,
                    on_manager_session_lost=self._on_manager_session_lost
                ),
                host=client_settings.server_host,
                port=client_settings.server_port,
            )

        task = self.aexit_context.create_task(
            _create_connection()
        )

        @task.add_done_callback
        def callback(f: Future) -> None:
            nonlocal retries
            try:
                _, p = f.result()
            except:
                retries += 1
                logger.debug(f'【{client_settings.name}】Manager Server Connected Failed - retry {retries} times')
                self.run_client(retries, 1)

    def run_worker(self) -> None:
        for idx in range(client_settings.workers):
            input_queue, output_queue = Queue(), Queue()
            message_keeper = ProcessQueueMessageKeeper(self, input_queue, output_queue)
            self.message_keepers.append(message_keeper)
            pro = Process(
                target=run_worker,
                args=(ClientWorker, ProcessQueueMessageKeeper, output_queue, input_queue)
            )
            pro.daemon = True
            pro.start()
            self.processes.append(pro)
            message_keeper.listen()
        self.iter_message_keeper = loop_travel(self.message_keepers)

    def _on_manager_session_made(self, token: str, proxy_ports: List[int], size: int) -> None:
        """manager连接成功，通知子进程连接proxy"""
        for port in proxy_ports:
            message_keeper: ProcessQueueMessageKeeper = next(self.iter_message_keeper)
            message_keeper.send(
                Message(
                    Event.PROXY_CREATE, port, token, size=size
                )
            )

    def _on_manager_session_lost(self, reason: int) -> None:
        """manager 断开连接"""
        broadcast_message(self.message_keepers, Event.MANAGER_DISCONNECT)
        if reason == CloseReason.UN_EXPECTED:
            logger.info(f'【{client_settings.name}】disconnected unexpected retry')
            self.run_client()
        if reason == CloseReason.MANAGER_ALREADY_CONNECTED:
            logger.info(f'【{client_settings.name}】already connected, close ~~')
            self.exit()
        elif reason == CloseReason.AUTH_FAIL:
            logger.info(f'【{client_settings.name}】auth fail, close ~~')
            self.exit()
        elif reason == CloseReason.PING_TIMEOUT:
            logger.info(f'【{client_settings.name}】ping timeout, retry')
            self.run_client()

    def exit(self):
        for pro in self.processes:
            pro.terminate()
        self.aexit_context.cancel_all()
        self._loop.stop()


if __name__ == '__main__':
    Client().run()
