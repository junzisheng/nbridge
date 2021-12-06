from typing import Iterator


def ignore(*args, **kwargs):
    pass


def loop_travel(iterator: Iterator) -> Iterator:
    while True:
        for i in iterator:
            yield i
