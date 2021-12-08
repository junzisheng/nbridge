import signal


class CloseReason(object):
    UN_EXPECTED = 0
    AUTH_FAIL = 1
    MANAGER_ALREADY_CONNECTED = 2
    PING_TIMEOUT = 3
    CLIENT_DISCONNECT = 4
    SERVER_CLOSE = 5


HANDLED_SIGNALS = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)
