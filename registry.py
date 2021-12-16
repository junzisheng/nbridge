from typing import Optional

from protocols import BaseProtocol


class Registry(object):
    def __init__(self):
        self._registry = {}

    def all_registry(self) -> dict:
        return self._registry

    def get(self, key: str) -> Optional[BaseProtocol]:
        return self._registry.get(key)

    def is_registered(self, key: str) -> bool:
        return key in self._registry

    def register(self, key: str, protocol: BaseProtocol) -> bool:
        if key in self._registry:
            return False
        self._registry[key] = protocol
        return True

    def unregister(self, key: str) -> bool:
        try:
            del self._registry[key]
            return True
        except KeyError:
            return False

