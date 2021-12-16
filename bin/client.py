from typing import List, Optional, Iterable, Dict, Union
from collections import defaultdict
import asyncio
from asyncio import Future
from multiprocessing import Process, Pipe
from functools import partial, cached_property
from optparse import OptionValueError, OptionParser
import os

from loguru import logger

from common_bases import Bin
from messager import Event, Message, ProcessPipeMessageKeeper, broadcast_message
from constants import CloseReason
from manager.client import ClientProtocol, ClientConnector
from worker.bases import run_worker, ClientWorkerStruct
from worker.client import ClientWorker

from config.settings import ClientSettings


def get_client_settings() -> ClientSettings:
    parser = OptionParser()
    parser.add_option('-w', '--works', dest="workers", default=os.cpu_count() * 2 + 1,
                      help="set client workers;default os (cpu_count * 2 + 1)", type=int)
    parser.add_option('-n', '--name', dest="name", help="client name;required", type=str)
    parser.add_option('-t', '--token', dest="token", help="client token;required", type=str)
    parser.add_option('-s', '--sh', dest="host", help="server net bridge host", type=str)
    parser.add_option('-p', '--port', dest="port", help="server net bridge post", type=int, default=9999)

    (options, args) = parser.parse_args()
    workers = options.workers
    name = options.name
    token = options.token
    host = options.host
    port = options.port

    if workers < 1:
        raise OptionValueError('workers must gather than 1!')
    elif not name:
        raise OptionValueError('name opt is required!')
    elif not token:
        raise OptionValueError('token opt is required!')
    elif not host:
        raise OptionValueError('host opt is required!')
    # os.environ['workers'] = str(workers)
    # os.environ['name'] = name
    # os.environ['token'] = token
    # os.environ['server_host'] = host
    # os.environ['server_port'] = str(port)

    return ClientSettings(
        workers=workers,
        name=name,
        token=token,
        server_host=host,
        server_port=port
    )


client_settings = get_client_settings()


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
                args=(ClientWorker, client_settings, ProcessPipeMessageKeeper, child_input_channel, parent_output_channel)
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
                client_settings=client_settings,
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
                Event.PROXY_CREATE,
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
