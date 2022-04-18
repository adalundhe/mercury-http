import math
from typing import List
from mercury_http.common.timeouts import Timeouts
from .connection import HTTP2Connection


class HTTP2Pool:

    def __init__(self, size: int, timeouts: Timeouts, reset_connections=False) -> None:
        self.size = size
        self.connections: List[HTTP2Connection] = []
        self.timeouts = timeouts
        self.pools_count = math.ceil(size/100)
        self.reset_connections = reset_connections

    def create_pool(self) -> None:
        self.connections = [
            HTTP2Connection(self.timeouts, reset_connection=self.reset_connections) for _ in range(self.size)
        ]

    async def close(self):
        for connection in self.connections:
            await connection.close()
