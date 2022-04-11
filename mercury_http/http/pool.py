
from typing import List
from .connection import Connection


class Pool:

    def __init__(self, size: int) -> None:
        self.size = size
        self.connections: List[Connection] = []

    def create_pool(self) -> None:
        for _ in range(self.size):
            self.connections.append(
                Connection()
            )
