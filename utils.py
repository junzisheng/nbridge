from typing import Iterator, Any, Dict, Union, List, Tuple
from itertools import chain


def ignore(*args, **kwargs):
    pass


def loop_travel(iterator: Iterator) -> Iterator:
    while True:
        for i in iterator:
            yield i


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



