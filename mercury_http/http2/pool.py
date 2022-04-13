import math
from typing import List
from mercury_http.common.timeouts import Timeouts
from .connection import HTTP2Connection


class HTTP2Pool:

    def __init__(self, size: int, timeouts: Timeouts) -> None:
        self.size = size
        self.connections: List[HTTP2Connection] = []
        self.timeouts = timeouts
        self.pools_count = math.ceil(size/100)

    def create_pool(self) -> None:
        self.connections = [
            HTTP2Connection(self.timeouts) for _ in range(self.size)
        ]

    async def close(self):
        for connection in self.connections:
            await connection.close()
