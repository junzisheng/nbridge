import multiprocessing
from multiprocessing.context import SpawnProcess
from multiprocessing import Process, process
from typing import Callable, Optional
import sys

import os

multiprocessing.allow_connection_pickling()
# spawn = multiprocessing.get_context("spawn")


def get_subprocess(target: Callable[..., None], *args, **kwargs) -> Process:
    stdin_fileno: Optional[int]
    try:
        stdin_fileno = sys.stdin.fileno()
    except OSError:
        stdin_fileno = None
    return Process(target=target, args=args, kwargs=kwargs)


def subprocess_started(target: Callable[..., None], stdin_fileno: Optional[int], args, kwargs):
    if stdin_fileno is not None:
        sys.stdin = os.fdopen(stdin_fileno)

    target(*args, **kwargs)

