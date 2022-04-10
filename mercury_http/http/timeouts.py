class Timeouts:

    def __init__(self, connect_timeout=10, socket_read_timeout=10, total_timeout=30) -> None:
        self.connect_timeout = connect_timeout
        self.socket_read_timeout = socket_read_timeout
        self.total_timeout = total_timeout