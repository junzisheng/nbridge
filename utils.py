from typing import Iterator, Any


def ignore(*args, **kwargs):
    pass


def loop_travel(iterator: Iterator) -> Iterator:
    while True:
        for i in iterator:
            yield i


def safe_remove(lst: list, value: Any) -> bool:
    try:
        lst.remove(value)
        return True
    except ValueError:
        return False
