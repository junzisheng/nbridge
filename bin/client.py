from typing import List, Optional, Iterable, Dict, Union
import asyncio
from asyncio import Future
from multiprocessing import Process, Pipe
from functools import partial, cached_property

from loguru import logger

from common_bases import Bin
from messager import Message, ProcessPipeMessageKeeper, broadcast_message
from constants import CloseReason
from manager.client import ClientProtocol, ClientConnector
from worker import Event, run_worker
from config.settings import client_settings
from manager.manager_worker import ClientWorker
from utils import loop_travel


class Client(Bin):
    def __init__(self) -> None:
        super(Client, self).__init__()
        self.process_map: Dict[int, Dict[str, Union[Process, ProcessPipeMessageKeeper]]] = {}
        self.client: Optional[ClientProtocol] = None

    def start(self) -> None:
        self.run_worker()
        self.run_client()

    @cached_property
    def message_keepers(self):
        return [p['message_keeper'] for p in self.process_map.values()]

    @cached_property
    def iter_message_keeper(self) -> Iterable:
        return loop_travel(self.message_keepers)

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
            self.process_map[pro.pid] = {
                'process': pro,
                'message_keeper': message_keeper
            }
            message_keeper.listen()

    def run_client(self) -> None:
        connector = ClientConnector()
        connector.connect(
            partial(
                ClientProtocol,
                on_manager_session_made=self._on_manager_session_made,
                on_manager_session_lost=self._on_manager_session_lost
            ),
            host=client_settings.server_host,
            port=client_settings.server_port,
        )
        f = self.aexit.create_future()

        @f.add_done_callback
        def on_close(r: Future) -> None:
            if r.cancelled():
                connector.abort()
            else:
                print(r.result())

        connector.set_waiter(f)

    def _on_manager_session_made(self, client: ClientProtocol, token: str, proxy_ports: List[int], size: int) -> None:
        """manager连接成功，通知子进程连接proxy"""
        self.client = client
        for port in proxy_ports:
            message_keeper: ProcessPipeMessageKeeper = next(self.iter_message_keeper)
            message_keeper.send(
                Message(
                    Event.PROXY_CREATE, port, token, size=size
                )
            )

    def _on_manager_session_lost(self, reason: int) -> None:
        """manager 断开连接"""
        # todo waiter message wait all process clean
        if self._stop:
            return
        broadcast_message(self.message_keepers, Event.MANAGER_DISCONNECT)
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

    async def do_handle_stop(self) -> None:
        if self.client:
            self.client.transport.close()
        logger.info('Public Servers Disconnect')
        for pro_map in self.process_map.values():
            message_keeper = pro_map['message_keeper']
            pro = pro_map['process']
            message_keeper.send(Message(Event.SERVER_CLOSE))
            message_keeper.stop()
            pro.join()
            message_keeper.close()
            logger.info(f'Process【{pro.pid}】Closed')
        # for all task cancelled
        await asyncio.sleep(0)


if __name__ == '__main__':
    Client().run()
