from protocols import BaseProtocol


class Registry(object):
    def __init__(self):
        self._registry = {}

    def is_registered(self, host_name: str) -> bool:
        return host_name in self._registry

    def register(self, host_name: str, protocol: BaseProtocol) -> bool:
        if host_name in self._registry:
            raise
        self._registry[host_name] = protocol
        return True

    def unregister(self, host_name: str) -> bool:
        try:
            del self._registry[host_name]
            return True
        except KeyError:
            return False


