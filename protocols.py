from typing import Callable, Optional, Type, Tuple, Dict, List, Union, Any
import pickle
from asyncio import Protocol, Task, transports, Future, CancelledError, TimerHandle
from struct import pack, unpack, calcsize
import asyncio

from loguru import logger

from constants import CloseReason
from invoker import Invoker, TypeInvoker
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
        content = pickle.dumps((m, args, kwargs))
        return pack(self.struct_format, len(content)) + content


class BaseProtocol(Protocol):
    invoker_bases: Tuple[Type[Invoker]] = (Invoker,)

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self.parser = Parser()
        self.transport: Optional[transports.Transport] = None
        self.invoker: TypeInvoker = self.create_invoker()
        self.aexit_context = AexitContext()
        self.close_reason: int = CloseReason.UN_EXPECTED

    def connection_made(self, transport: transports.Transport) -> None:
        self.transport = transport
        self.on_connection_made()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.on_connection_lost(exc)
        self.invoker.on_protocol_close()
        self.aexit_context.cancel_all()

    def on_connection_made(self) -> None: pass

    def on_connection_lost(self, exc: Optional[Exception]) -> None: pass

    def set_close_reason(self, reason: int) -> None:
        assert self.close_reason == CloseReason.UN_EXPECTED or self.close_reason == reason
        self.close_reason = reason

    def create_invoker(self) -> TypeInvoker:
        return type('Invoker', self.invoker_bases, self.get_invoker_attrs())(self)

    def get_invoker_attrs(self) -> Dict:
        return {}

    def data_received(self, data: bytes) -> None:
        try:
            self.parser.dumps(data, self.on_message)
        except Exception as e:
            self._loop.call_exception_handler({'message': 'packet dumps fail', 'exception': e})
            self.transport.close()

    def remote_call(self, m: callable, *args, **kwargs) -> None:
        if not self.transport.is_closing():
            self.transport.write(self.parser.loads(m.__name__, *args, **kwargs))

    def remote_multi_call(self, call_list: List[Tuple[callable, tuple, dict]]) -> None:
        _call_list = []
        for c, a, k in call_list:
            _call_list.append((c.__name__, a, k))
        self.remote_call(
            BaseProtocol.rpc_multi,
            _call_list
        )
        
    def close(self, reason: int) -> None:
        if not self.transport.is_closing():
            self.set_close_reason(reason)
            self.remote_call(
                BaseProtocol.rpc_set_close_reason,
                reason
            )
            self.transport.close()

    def call(self, m: str, *args, **kwargs) -> Any:
        try:
            return getattr(self, m)(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            self.transport.close()

    def on_message(self, data: bytes) -> None:
        m, args, kwargs = pickle.loads(data)
        # invoker
        if m.startswith('call_'):
            self.invoker.call(m, *args, **kwargs)
        # protocol
        elif m.startswith('rpc_'):
            self.call(m, *args, **kwargs)
        else:
            message = f'Unknown rpc method {m}'
            self._loop.call_exception_handler({'message': message, 'exception': Exception(message)})

    # base rpc method
    def rpc_multi(self, call_list: List[Tuple[str, tuple, dict]]) -> None:
        for c, a, k in call_list:
            if c.startswith('call_'):
                self.invoker.call(c, *a, **k)
            elif c.startswith('rpc_'):
                self.call(c, *a, **k)
            else:
                raise RuntimeError(f'Unknown rpc method:{c}')

    def rpc_auth_fail(self, *args, **kwargs) -> None:
        self.close(CloseReason.AUTH_FAIL)

    def rpc_auth_success(self, *args, **kwargs) -> None: pass

    def rpc_set_close_reason(self, reason: int) -> None:
        self.set_close_reason(reason)

    @staticmethod
    def rpc_log(_log: str) -> None:
        logger.info(_log)


class Connector(object):
    def __init__(self, factory: Callable[..., Protocol], host: str, port: int) -> None:
        self._loop = asyncio.get_event_loop()
        self.context: Dict = {}
        self.task: Union[TimerHandle, Task, Optional] = None
        self.factory = factory
        self.host = host
        self.port = port

    def connect(self, waiter_factory: Optional[Callable[..., Future]] = None) -> Task:
        if self.task is not None:
            raise RuntimeError(f'{self.__class__} is connecting!')

        self.task = self._loop.create_task(
            self._loop.create_connection(self.factory, host=self.host, port=self.port)
        )

        @self.task.add_done_callback
        def task_callback(f: Future):
            waiter = waiter_factory() if waiter_factory else None
            try:
                _, protocol = f.result()
                if waiter:
                    waiter.set_result(protocol)
            except CancelledError:
                if waiter:
                    waiter.cancel()
            except Exception as e:
                self.client_connection_failed(e)
                if waiter:
                    waiter.set_exception(e)
            finally:
                self.task = None

        return self.task

    def client_connection_failed(self, reason: Exception) -> None:
        pass

    def abort(self) -> None:
        if self.task and not self.task.done():
            self.task.cancel()


class ReConnector(Connector):
    delay = 1
    retries = 0
    continue_try = 1
    timer: Optional[TimerHandle] = None

    def client_connection_made(self, protocol: Protocol) -> None:
        pass

    def client_connection_failed(self, reason: Exception) -> None:
        super(ReConnector, self).client_connection_failed(reason)
        self.retry()

    def retry(self) -> None:
        if self.continue_try:
            self.retries += 1
            self.timer = self._loop.call_later(self.delay, self.connect)

    def stop_retry(self) -> None:
        self.continue_try = 0

    def abort(self) -> None:
        self.continue_try = False
        if self.timer:
            self.timer.cancel()
        super(ReConnector, self).abort()

