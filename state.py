class State(object):
    WAIT_AUTH = "WAIT_AUTH"
    IDLE = "IDLE"
    WORK = "WORK"
    WAIT_CLIENT_READY = "WAIT_CLIENT_READY"
    DISCONNECT = "DISCONNECT"

    def __init__(self):
        self.st = self.WAIT_AUTH

    def to(self, st) -> None:
        self.st = st


