from typing import Callable, Optional, Any, Type, Tuple, TYPE_CHECKING, Dict, List
import pickle
from asyncio import Protocol, transports
from struct import pack, unpack, calcsize

import asyncio

from constants import CloseReason
from revoker import Revoker, TypeRevoker
from aexit_context import AexitContext, TaskType


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


