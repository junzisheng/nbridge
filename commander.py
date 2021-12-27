from typing import Union, TYPE_CHECKING, Dict, Optional, List, Any, Tuple
import threading
import asyncio
import os
import socket
import uuid
from asyncio.futures import Future

from fire import Fire

from protocols import BaseProtocol
from config.settings import BASE_DIR
if TYPE_CHECKING:
    from bin.server import ProxyExecutor


ALL_CLIENT = b'ALL_CLIENT'


class Client:
    def __init__(self):
        sock_file = os.path.join(BASE_DIR, 'config', './bridge.sock')
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(sock_file)
        self._loop = asyncio.get_event_loop()
        _, self.client = self._loop.run_until_complete(
            self._loop.create_connection(
                Commander,
                sock=sock
            )
        )  # type: Any, Commander


class Commander(BaseProtocol):
    waiters: Dict[str, Future] = {}

    def command_call(self, command: str, *args, **kwargs) -> Future:
        waiter_uid = uuid.uuid4().hex
        self.remote_call(
            CommanderServer.rpc_command,
            command,
            waiter_uid,
            *args,
            **kwargs
        )
        f = self._loop.create_future()
        self.waiters[waiter_uid] = f
        return f

    def rpc_response(self, waiter_id: str, response: Dict) -> None:
        self.waiters[waiter_id].set_result(response)


class CommanderServer(Commander):

    def __init__(self, executor: 'ProxyExecutor') -> None:
        super(CommanderServer, self).__init__()
        self.executor = executor

    def rpc_command(self, command, waiter_id: str, *args, **kwargs) -> None:
        res = getattr(self.executor, command)(*args, **kwargs)  # type: Future
        if isinstance(res, Future):
            @res.add_done_callback
            def _(f):
                self.remote_call(
                    Commander.rpc_response,
                    waiter_id,
                    f.result()
                )
        else:
            self.remote_call(
                Commander.rpc_response,
                waiter_id,
                res
            )


class CommanderClient(Client):
    async def ls(self) -> None:
        res = await self.client.command_call(
            'ls_client'
        )
        print(res)

    async def add(self, client_name: str) -> None:
        if '/' in client_name or '@' in client_name or ':' in client_name:
            print('client_name cant contain【/,@:】')
            return
        res = await self.client.command_call(
            'add_client',
            str(client_name)
        )
        print(res)

    async def rm(self, client_name: str) -> None:
        res = await self.client.command_call(
            'rm_client',
            str(client_name)
        )
        print(res)


class CommanderPublicTcp(Client):
    async def add(self, client_name: str, local_addr: str, bind_port=0) -> None:
        res = await self.client.command_call(
            'add_tcp',
            str(client_name),
            local_addr.split(':'),
            bind_port
        )
        print(res)

    async def ls(self):
        res = await self.client.command_call(
            'ls_tcp',
        )
        print(res)

    async def rm(self, bind_port: int) -> None:
        waiter = self.client.command_call(
            'rm_tcp',
            bind_port
        )
        print(await waiter)


if __name__ == '__main__':
    Fire({
        'client': CommanderClient,
        'tcp': CommanderPublicTcp
    })









