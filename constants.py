import signal

HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)


class ProxyState(object):
    INIT = "INIT"
    IDLE = "IDLE"
    BUSY = "BUSY"


class CloseReason(object):
    UN_EXPECTED = 0
    AUTH_FAIL = 1
    MANAGER_ALREADY_CONNECTED = 2
    MANAGE_CLIENT_LOST = 4
    PING_TIMEOUT = 3
    SERVER_CLOSE = 5
    CLIENT_NAME_UNKNOWN = 6
    CLINET_EPOCH_EXPIRED = 7

