import multiprocessing
from multiprocessing.context import SpawnProcess
from typing import Callable, Optional
import sys

import os

multiprocessing.allow_connection_pickling()
spawn = multiprocessing.get_context("spawn")


def get_subprocess(target: Callable[..., None], *args, **kwargs) -> SpawnProcess:
    stdin_fileno: Optional[int]
    try:
        stdin_fileno = sys.stdin.fileno()
    except OSError:
        stdin_fileno = None
    return spawn.Process(target=subprocess_started, kwargs={'target': target,
                                                            'stdin_fileno': stdin_fileno,
                                                            'args': args, 'kwargs': kwargs})


def subprocess_started(target: Callable[..., None], stdin_fileno: Optional[int], args, kwargs):
    if stdin_fileno is not None:
        sys.stdin = os.fdopen(stdin_fileno)

    target(*args, **kwargs)

