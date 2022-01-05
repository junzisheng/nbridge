import asyncio
import threading
from typing import Dict, List, Optional, Union, Tuple
import uuid
import os
from asyncio import Future
from asyncio.futures import _chain_future
from asyncio.base_events import Server as Aserver
from multiprocessing import Pipe, Process
from functools import partial, cached_property

import socket
from loguru import logger
from tinydb import Query, where
from tinydb.operations import set

from common_bases import Bin, Client
from constants import CloseReason, ManagerState
from messager import Event
from worker.bases import run_worker, ServerWorkerStruct
from worker.server import ManagerWorker
from registry import Registry
from messager import ProcessPipeMessageKeeper, SocketChannel, Message, gather_message, broadcast_message, broadcast_socket
from commander import CommanderServer
from utils import create_server_sock
from manager.server import ManagerServer
from config.settings import server_settings, BASE_DIR, client_table, public_table


class ConfigError(Exception):
    pass


class Server(Bin):
    def __init__(self) -> None:
        super(Server, self).__init__()
        self.workers: Dict[int, ServerWorkerStruct] = {}
        self.manager_server: Optional[Aserver] = None

    def start(self) -> None:
        self.run_manager()
        self.run_worker()
        # self.run_public_server()
        self.run_commander()

    def run_manager(self) -> None:
        def on_manager_session_made(client_name: str, f: Future) -> None:
            client = client_table.get(where('client_name') == client_name)
            gather = gather_message(
                self.message_keepers, Event.MANAGER_SESSION_MADE, client_name=client_name,
                epoch=client['epoch']
            )
            _chain_future(gather, f)

        def on_manager_session_lost(protocol: ManagerServer) -> None:
            client_name = protocol.client_name
            if protocol.close_reason != CloseReason.COMMANDER_REMOVE:
                gather = gather_message(
                    self.message_keepers,
                    Event.MANAGER_SESSION_LOST,
                    client_name=client_name,
                )
                self.aexit.monitor_future(gather)

                @gather.add_done_callback
                def _(_):
                    client_table.update(
                        set('state', ManagerState.idle),
                        where('client_name') == client_name,
                    )

        self.manager_server = self._loop.run_until_complete(
            self._loop.create_server(
                partial(
                    ManagerServer,
                    self.workers,
                    on_session_made=on_manager_session_made,
                    on_session_lost=on_manager_session_lost,
                ),
                host=server_settings.manager_bind_host,
                port=server_settings.manager_bind_port,
            )
        )
        logger.info(f'Manager Server - [{server_settings.manager_bind_host}:{server_settings.manager_bind_port}] - '
                    f'{os.getpid()}: Started')

        @self.aexit.add_callback_when_cancel_all
        def _():
            for client in client_table.all():
                client_prt = client['client']
                if client_prt:
                    client_prt.close(CloseReason.SERVER_CLOSE)
                    logger.info(f'{client["client_name"]} Disconnect Done')
            self.manager_server.close()

    def run_public_server(self) -> None:
        for public in server_settings.public_list:
            if public.type == 'tcp':
                client_name = list(public.mapping.keys())[0]
                local_addr = list(public.mapping.values())[0]
                try:
                    self.add_tcp(client_name, local_addr, public.bind_port)
                except ConfigError as e:
                    logger.error(str(e))
                    self.closer.call_close()
                    return

    def run_commander(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock_file = os.path.join(BASE_DIR, 'config', './bridge.sock')
        try:
            os.unlink(sock_file)
        except FileNotFoundError:
            pass
        sock.bind(sock_file)
        server: Aserver = self._loop.run_until_complete(
            self._loop.create_server(
                partial(
                    CommanderServer,
                    ProxyExecutor(self),
                ),
                sock=sock
            )
        )

        @self.aexit.add_callback_when_cancel_all
        def _():
            server.close()
            os.unlink(sock_file)

    def run_worker(self) -> None:
        for idx in range(server_settings.workers):
            # 进程通信通道
            parent_input_channel, parent_output_channel = Pipe()
            child_input_channel, child_output_channel = Pipe()
            message_keeper = ProcessPipeMessageKeeper(self.make_receiver(), parent_input_channel, child_output_channel)
            socket_input, socket_output = Pipe()
            pro = Process(
                target=run_worker,
                args=(
                    ManagerWorker, ProcessPipeMessageKeeper,
                    child_input_channel, parent_output_channel,
                    socket_output,
                )
            )
            pro.daemon = True
            pro.start()
            child_input_channel.close()
            parent_output_channel.close()
            socket_output.close()
            # 进程随机port启动proxy服务，这里获取port
            proxy_port = message_keeper.get()

            message_keeper.listen()
            self.workers[pro.pid] = ServerWorkerStruct(
                proxy_port=proxy_port,
                socket_channel=SocketChannel(socket_input, pro.pid),
                process=pro,
                message_keeper=message_keeper,
            )
            logger.info(f'Proxy Server - 【{server_settings.manager_bind_host}:{proxy_port}】 - {pro.pid}: Started')

    def add_tcp(self, client_name: str, local_addr: Tuple[str, int], bind_port: int = 0) -> Tuple[Future, int]:
        client = public_table.get(where('client_name') == client_name)
        if client is None:
            raise ConfigError(f'client: {client_name} not add')
        if bind_port != 0:
            if public_table.search(where('port') == bind_port).count() > 0:
                raise ConfigError(f'bind port: {bind_port} already in use')
        sock = create_server_sock((server_settings.manager_bind_host, bind_port))
        public_table.insert({
            'port': sock.getsockname()[1], 'type': 'tcp', 'client_name': client_name,
            'custom_domain': '', 'local_host': local_addr[0], 'local_port': local_addr[1]
        })
        gather = gather_message(
            self.message_keepers,
            Event.TCP_ADD,
            client_name,
            local_addr,
        )
        broadcast_socket(self.socket_senders, sock.fileno())
        sock.close()
        return gather, bind_port

    def add_http(
        self, client_name: str, local_addr: Tuple[str, int], custom_domain: str, bind_port: int = 0
    ) -> Tuple[Future, int]:
        sock = None
        client = public_table.get(where('client_name') == client_name)
        if client is None:
            raise ConfigError(f'client: {client_name} not add')
        if bind_port == 0:
            sock = create_server_sock((server_settings.manager_bind_host, 0))
            bind_port = sock.getsockname()[1]
            config = self.public_map[bind_port]
            config.update({'type': 'http', 'mapping': {custom_domain: (client_name, local_addr)}})
        elif not self.public_map[bind_port]:
            sock = create_server_sock((server_settings.manager_bind_host, bind_port))
            config = self.public_map[bind_port]
            config.update({'type': 'http', 'mapping': {custom_domain: (client_name, local_addr)}})
        else:
            config = self.public_map[bind_port]
            if config['type'] != 'http':
                raise ConfigError(f'bind port: {bind_port} already in use type: tcp')
            if config['mapping'].get(custom_domain):
                raise ConfigError(f'custom domain: {custom_domain} already set')
            config['mapping'][custom_domain] = (client_name, local_addr)
        gather = gather_message(
            self.message_keepers,
            Event.HTTP_ADD,
            client_name,
            local_addr,
            custom_domain,
            sock=True if sock else False
        )
        if sock:
            broadcast_socket(self.socket_senders, sock.fileno())
            sock.close()
        return gather, bind_port

    @cached_property
    def all_worker(self) -> List[ServerWorkerStruct]:
        return list(self.workers.values())

    @cached_property
    def message_keepers(self) -> List[ProcessPipeMessageKeeper]:
        return [w.message_keeper for w in self.workers.values()]

    @cached_property
    def socket_senders(self) -> List[SocketChannel]:
        return [w.socket_channel for w in self.workers.values()]

    async def do_handle_stop(self) -> None:
        workers = self.workers.values()
        for worker in workers:
            worker.put(Message(Event.SERVER_CLOSE))
            worker.message_keeper.stop()
        for worker in workers:
            worker.process.join()
            worker.message_keeper.close()
            logger.info(f'Process【{worker.pid}】Closed')
        # 关闭manager client
        logger.info('Manager Server Closed')

    def make_receiver(self) -> Dict:
        def proxy_create(client_name: str, port, num=1) -> None:
            manager = self.manager_registry.get(client_name)  # type: ManagerServer
            if manager and manager.session_status == 1:
                manager.create_proxy(port, num)

        return {
            Event.PROXY_CREATE: proxy_create
        }


_commander_lock = threading.Lock()


def _commander_lock_wrapper(func):
    def _wrapper(*args, **kwargs):
        try:
            _commander_lock.acquire()
            res = func(*args, **kwargs)
            if isinstance(res, Future):
                res.add_done_callback(lambda _: _commander_lock.release())
            else:
                _commander_lock.release()
            return res
        except Exception as e:
            asyncio.get_event_loop().call_exception_handler(
                {'exception': e}
            )
            _commander_lock.release()
    return _wrapper


class ProxyExecutor(object):
    def __init__(self, executor: Server):
        self.executor: Server = executor

    # ============ client  ============
    @_commander_lock_wrapper
    def add_client(self, client_name: str) -> Dict:
        client = client_table.get(where('client_name') == client_name)
        if client:
            return {'code': -1, 'reason': f'{client_name} already added'}
        else:
            token = uuid.uuid4().hex
            client = {
                'client_name': client_name, "token": token, "state": ManagerState.idle, "epoch": 0, "client": None
            }
            client_table.insert(client)
            broadcast_message(
                self.executor.message_keepers,
                Event.CLIENT_ADD,
                client_name,
                token
            )
            return {'code': 1, 'token': token}

    @_commander_lock_wrapper
    def rm_client(self, client_name: str):
        client = client_table.get(where('client_name') == client_name)
        if not client:
            return {'code': -1, 'reason': f'{client_name} does not exist'}
        client_table.remove(where('client_name') == client_name)
        # 关闭public
        public_table.remove(where('client_name') == client_name)
        manager = client['client']
        if manager:
            manager.close(CloseReason.COMMANDER_REMOVE)
        gather = gather_message(
            self.executor.message_keepers,
            Event.CLIENT_REMOVE,
            client_name=client_name
        )
        f = Future()

        @gather.add_done_callback
        def _(_):
            f.set_result({'code': 1, 'reason': f'{client_name} remove done'})
        return f

    @staticmethod
    def ls_client() -> List[Dict]:
        all_clients = client_table.all()
        for c in all_clients:
            del c['client']
        return all_clients
        # return [x for x in client_table.all()]

    # ============ public ============
    def ls_public(self, client_name: Union[str, bytes]) -> None:
        pass

    # ============ public.tcp ============
    @_commander_lock_wrapper
    def add_tcp(self, client_name: str, local_addr: Tuple[str, int], bind_port=0) -> Union[str, Future]:
        try:
            gather, port = self.executor.add_tcp(client_name, local_addr, bind_port)
        except ConfigError as e:
            return str(e)
        f = Future()

        @gather.add_done_callback
        def _(_):
            f.set_result(port)
        return f

    @staticmethod
    def ls_tcp() -> List[Dict]:
        return public_table.search(where('type') == 'tcp')

    def rm_tcp(self, bind_port: int) -> Union[Future, Dict]:
        doc_ids = public_table.remove((where('port') == bind_port) & (where('type') == 'tcp'))
        if doc_ids:
            gather = gather_message(
                self.executor.message_keepers,
                Event.TCP_REMOVE,
                bind_port
            )
            f = Future()

            @gather.add_done_callback
            def _(_):
                f.set_result({'code': 1, 'reason': f'tpc: {bind_port} rm done'})
            return f
        else:
            return {'code': -1, 'reason': f'tpc: {bind_port} does not add'}

    # ============ public.http ============
    @_commander_lock_wrapper
    def add_http(
        self, client_name: str, local_addr: Tuple[str, int], custom_domain: str, bind_port: int = 0
    ) -> Union[str, Future]:
        try:
            gather, port = self.executor.add_http(client_name, local_addr, custom_domain, bind_port)
        except ConfigError as e:
            return str(e)
        f = Future()

        @gather.add_done_callback
        def _(_):
            f.set_result(port)
        return f

    @_commander_lock_wrapper
    def rm_http(self) -> bool:
        pass


if __name__ == '__main__':
    Server().run()