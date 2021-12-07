from typing import List, Optional, Iterable
import os
import asyncio
from asyncio import Queue, Task, Future
from multiprocessing import Queue as Mqueue
from multiprocessing.context import SpawnProcess
from functools import partial
from concurrent.futures import ThreadPoolExecutor

from loguru import logger

from messager import ProcessMessageKeeper, Message
from constants import CloseReason
from bridge_manager.client import ClientProtocol
from worker import Event, create_event
from sub_process import get_subprocess
from settings import settings
from bridge_manager.worker import ClientWorker
from utils import loop_travel

host_name = 'test_host'


class Client(object):
    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self.queue = Queue()  # 和client通信
        self.message_keepers: [ProcessMessageKeeper] = []
        self.processes: List[SpawnProcess] = []
        self._loop.set_default_executor(
            ThreadPoolExecutor(max_workers=min(settings.workers * 2 + 1, 32, (os.cpu_count() or 1)+4))
        )
        self.iter_message_keeper: Optional[Iterable] = None
        self.tasks: List[Task] = []

    def run(self) -> None:
        self.run_worker()
        self.run_client()
        self.tasks.append(self._loop.create_task(self.listen_client_queue()))
        self._loop.run_forever()

    def run_client(self, retries=0, delay=0) -> None:
        async def _create_connection(delay=0):
            if delay > 0:
                await asyncio.sleep(delay)
            return await self._loop.create_connection(
                partial(ClientProtocol, self.queue, host_name),
                host=settings.manager_remote_host,
                port=settings.manager_bind_port,
            )

        task = self._loop.create_task(
            _create_connection(delay)
        )
        self.tasks.append(task)

        @task.add_done_callback
        def callback(f: Future) -> None:
            self.tasks.remove(task)
            nonlocal retries
            try:
                _, p = f.result()
            except Exception as e:
                retries += 1
                logger.debug(f'【{host_name}】Manager Server connected failed;retry {retries} times')
                self.run_client(retries, 1)

    def run_worker(self) -> None:
        for idx in range(settings.workers):
            message_keeper = ProcessMessageKeeper(self, Mqueue(), Mqueue())
            self.message_keepers.append(message_keeper)
            pro = get_subprocess(ClientWorker.run, message_keeper.get_input_queue(), message_keeper.get_output_queue())
            self.processes.append(pro)
            pro.start()
            message_keeper.listen()
        self.iter_message_keeper = loop_travel(self.message_keepers)

    async def listen_client_queue(self) -> None:
        while True:
            event, args, kwargs = await self.queue.get()  # type: str, tuple, dict
            try:
                getattr(self, 'on_client_' + event.lower())(*args, **kwargs)
            except Exception as e:
                logger.exception(e)

    def on_client_proxy_create(self, port: int, token: str) -> None:
        """manager连接成功，通知子进程连接proxy"""
        message_keeper: ProcessMessageKeeper = next(self.iter_message_keeper)
        message_keeper.send(
            Message(
                Event.PROXY_CREATE, host_name, port, token, size=settings.worker_pool_size
            )
        )

    def on_client_manager_disconnect(self, reason: int) -> None:
        """manager 断开连接"""
        self.broadcast_process(
            Message(Event.MANAGER_DISCONNECT)
        )
        if reason == CloseReason.UN_EXPECTED:
            logger.info(f'【{host_name}】disconnected unexpected retry')
            self.run_client()
        if reason == CloseReason.MANAGER_ALREADY_CONNECTED:
            logger.info(f'【{host_name}】already connected, close ~~')
            self.exit()
        elif reason == CloseReason.AUTH_FAIL:
            logger.info(f'【{host_name}】auth fail, close ~~')
            self.exit()
        elif reason == CloseReason.PING_TIMEOUT:
            logger.info(f'【{host_name}】ping timeout, retry')
            self.run_client()

    def broadcast_process(self, message: Message) -> None:
        for message_keeper in self.message_keepers:
            message_keeper.send(message)

    def exit(self):
        for pro in self.processes:
            pro.terminate()
        for task in self.tasks:
            if not task.done():
                task.cancel()
        self._loop.stop()


if __name__ == '__main__':
    Client().run()
