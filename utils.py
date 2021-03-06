from typing import Iterator, Any, Union, Tuple, Callable, Awaitable
import os
import logging
import sys
import asyncio
from asyncio import constants, transports
import socket
import errno

from loguru import logger


def ignore(*args, **kwargs): pass


async def async_ignore(*args, **kwargs): pass


def safe_remove(lst: Union[list, dict], value: Any) -> bool:
    try:
        if isinstance(lst, list):
            lst.remove(value)
        else:
            del lst[value]
        return True
    except (ValueError, KeyError):
        return False


def map_chain(*lst: dict) -> dict:
    lst = list(lst)
    if not lst:
        return {}
    org = lst.pop(0)
    for d in lst:
        org.update(d)
    return org


def wrapper_prefix_key(prefix: str, target: dict) -> dict:
    r = {}
    for key, val in target.items():
        r[prefix+key] = val
    return r


def start_server(server_endpoint: Tuple[str, int], post: callable, backlog=100) -> socket.socket:
    loop = asyncio.get_event_loop()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    server.bind(server_endpoint)
    server.setblocking(False)
    server.listen(backlog)

    def _accept_connection():
        for i in range(backlog):
            try:
                conn, addr = server.accept()
                post(conn)
            except (BlockingIOError, InterruptedError, ConnectionAbortedError):
                return None
            except OSError as exc:
                if exc.errno in (errno.EMFILE, errno.ENFILE,
                                 errno.ENOBUFS, errno.ENOMEM):
                    loop.remove_reader(server.fileno())
                    loop.call_later(
                        constants.ACCEPT_RETRY_DELAY,
                        _start_serving
                    )

    def _start_serving():
        loop.add_reader(server.fileno(), _accept_connection)

    _start_serving()
    return server


def socket_fromfd(fd: int) -> socket.socket:
    return socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)


def create_server_sock(addr: Tuple[str, int], backlog=50) -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)
    sock.setblocking(False)
    sock.bind(addr)
    sock.listen(backlog)
    sock.set_inheritable(True)
    return sock


def catch_cor_exception(cor: Callable[..., Awaitable[Any]]):
    """????????????task?????????????????????del??????????????????exception_handler????????????????????????????????????"""
    async def wrapper(*args, **kwargs):
        try:
            return await cor(*args, **kwargs)
        except Exception as e:
            loop = asyncio.get_running_loop()
            loop.call_exception_handler({'message': 'unexpected error', 'exception': e})
    return wrapper


def protocol_sockname(transport: transports.Transport) -> Tuple[str, int]:
    sock: socket.socket = transport.get_extra_info('socket')
    return sock.getsockname()


def setup_logger(log_file_dir, log_file_name: str):
    logger.remove(handler_id=None)
    logger.add(
        sys.stdout, level=logging.INFO, format="{time:%Y-%m-%d %H:%M:%S} | {level} | {message}, {process}",
        filter=lambda record: record['level'].name == 'INFO'
    )
    logger.add(
        sys.stdout, level=logging.DEBUG, format="{time:%Y-%m-%d %H:%M:%S} | {level} | {message}, {process}",
        filter=lambda record: record['level'].name == 'DEBUG'
    )
    logger.add(
        sys.stdout, level=logging.ERROR, format="{time:%Y-%m-%d %H:%M:%S} | {level} | {message}, {process}",
        filter=lambda record: record['level'].name == 'ERROR'
    )
    logger.add(
        os.path.join(log_file_dir, f'{log_file_name}.info'), level=logging.INFO,
        format="{time:%Y-%m-%d %H:%M:%S} | {level} | {message}",
        filter=lambda record: record['level'].name == 'INFO', rotation="00:00"
    )
    logger.add(
        os.path.join(log_file_dir, f'{log_file_name}.error'),
        level=logging.ERROR, format="{time:%Y-%m-%d %H:%M:%S} | {level} | {message}", rotation="00:00"
    )


