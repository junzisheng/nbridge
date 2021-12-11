from typing import Callable, Optional, Type, Tuple, Dict, List, Union
import pickle
from asyncio import Protocol, Task, transports, Future, CancelledError, TimerHandle
from functools import partial, wraps
from struct import pack, unpack, calcsize
import asyncio

from loguru import logger

from constants import CloseReason
from revoker import Revoker, TypeRevoker
from aexit_context import AexitContext


class Parser(object):
    struct_format = "!I"
    prefix_length = calcsize(struct_format)

    def __init__(self):
        self.unprocessed = b''

    def dumps(self, data: bytes, callback: Callable[[bytes], None]) -> None:
        self.unprocessed += data
        offset = 0
        while len(self.unprocessed) >= (offset + self.prefix_length):
            message_start = offset + self.prefix_length
            (length,) = unpack(self.struct_format, self.unprocessed[offset:message_start])
            message_end = message_start + length
            if len(self.unprocessed) < message_end:
                break
            packet = self.unprocessed[message_start: message_end]
            offset = message_end
            callback(packet)
        self.unprocessed = self.unprocessed[offset:]

    def loads(self, m: str, *args, **kwargs) -> bytes:
        content = pickle.dumps({'m': m, 'args': args, 'kwargs': kwargs})
        return pack(self.struct_format, len(content)) + content


class BaseProtocol(Protocol):
    revoker_bases: Tuple[Type[Revoker]] = (Revoker,)

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self.parser = Parser()
        self.transport: Optional[transports.Transport] = None
        self.revoker: TypeRevoker = self.create_revoker()
        self.aexit_context = AexitContext()
        self.close_reason: int = CloseReason.UN_EXPECTED

    def connection_made(self, transport: transports.Transport) -> None:
        self.transport = transport
        self.on_connection_made()

    def on_connection_made(self) -> None:
        pass

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.on_connection_lost(exc)
        self.revoker.on_protocol_close()
        self.aexit_context.cancel_all()

    def on_connection_lost(self, exc: Optional[Exception]) -> None:
        pass

    def set_close_reason(self, reason: int) -> None:
        self.close_reason = reason

    def create_revoker(self) -> TypeRevoker:
        return type('Revoker', self.revoker_bases, self.get_revoker_attrs())(self)

    def get_revoker_attrs(self) -> Dict:
        return {}

    def data_received(self, data: bytes) -> None:
        self.parser.dumps(data, self.on_message)

    def rpc_call(self, m: callable, *args, **kwargs) -> None:
        if not self.transport.is_closing():
            self.transport.write(self.parser.loads(m.__name__[5:], *args, **kwargs))

    def rpc_multi_call(self, call_list: List[Tuple[callable, tuple, dict]]) -> None:
        from revoker import Revoker
        _call_list = []
        for c, a, k in call_list:
            _call_list.append((c.__name__[5:], a, k))
        self.rpc_call(
            Revoker.call_multi,
            _call_list
        )

    def on_message(self, data: bytes) -> None:
        try:
            pdata = pickle.loads(data)
            m, args, kwargs = pdata['m'], pdata['args'], pdata['kwargs']
        except:
            self.transport.close()
        else:
            self.revoker.call(m, *args, **kwargs)


class Connector(object):
    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self.context: Dict = {}
        self.task: Union[TimerHandle, Task, Optional] = None
        self._waiter: Optional[Future] = None

    def set_waiter(self, f: Future) -> None:
        self._waiter = f

    def connect(self, factory: Callable[..., Protocol], host: str, port: int) -> None:
        self.context = {
            'factory': factory,
            'host': host,
            'port': port
        }
        self.task = self._loop.create_task(
            self._loop.create_connection(
                factory,
                host=host,
                port=port,
            )
        )

        @self.task.add_done_callback
        def task_callback(f: Future):
            try:
                transport, protocol = f.result()
            except CancelledError:
                pass
            except Exception as e:
                self.client_connection_failed(e)
            else:
                self.client_connection_made(protocol)
                if self._waiter and not self._waiter.done():
                    self._waiter.set_result(protocol)

    def client_connection_made(self, protocol: Protocol) -> None:
        pass

    def log_fail(self, reason: Exception) -> None:
        logger.info(f'{self.__class__.__name__} - Connect Failed - {reason.__class__.__name__}')

    def client_connection_failed(self, reason: Exception) -> None:
        self.log_fail(reason)

    def abort(self) -> None:
        if self.task and not self.task.cancelled():
            self.task.cancel()
            if self._waiter:
                self._waiter.cancel()


class ReConnector(Connector):
    delay = 1
    retries = 0
    continue_try = 1
    mode = "until_connect"

    def __init__(self):
        """mode: until_connect一直重试到成功; always 断开连接也重试"""
        assert self.mode in ['until_connect', 'always']
        super(ReConnector, self).__init__()

    def client_connection_made(self, protocol: Protocol) -> None:
        if self.mode == "until_connect":
            return

        def __hook_connection_lost(reason: Exception) -> None:
            connection_lost(reason)
            self.log_fail(reason)
            self.retry()
        connection_lost = protocol.connection_lost
        wraps(connection_lost)(__hook_connection_lost)
        protocol.connection_lost = __hook_connection_lost

    def client_connection_failed(self, reason: Exception) -> None:
        super(ReConnector, self).client_connection_failed(reason)
        if self.continue_try:
            self.retry()

    def retry(self) -> None:
        self.retries += 1
        self.task = self._loop.call_later(self.delay, partial(self.connect, **self.context))

