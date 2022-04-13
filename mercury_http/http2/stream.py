import asyncio
from asyncio import StreamReader, StreamWriter
from traceback import TracebackException
from typing import Optional, Type
from mercury_http.http.timeouts import Timeouts
from .url import URL


class AsyncLock:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "AsyncLock":
        await self._lock.acquire()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_value: Optional[BaseException] = None,
        traceback: Optional[TracebackException] = None,
    ) -> None:
        self._lock.release()

class AsyncStream:
    READ_NUM_BYTES = 64 * 1024

    def __init__(self, timeouts: Timeouts) -> None:
        self.reader: StreamReader = None
        self.writer: StreamWriter = None
        self.timeouts = timeouts
        self._connected = False

    async def connect(self, url: URL):
        if self._connected is False:
            stream = await asyncio.wait_for(asyncio.open_connection(
                url.ip_addr, 
                url.port, 
                ssl=url.ssl
            ), self.timeouts.connect_timeout)

            self.reader, self.writer = stream

    def write(self, data: bytes):
        self.writer.write(data)

    async def read(self, msg_length: int=READ_NUM_BYTES):
        return await asyncio.wait_for(self.reader.read(msg_length), self.timeouts.total_timeout)
