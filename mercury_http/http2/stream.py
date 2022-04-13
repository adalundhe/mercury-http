import asyncio
from asyncio import StreamReader, StreamWriter
from mercury_http.common import Timeouts, Request


class AsyncStream:
    READ_NUM_BYTES = 64 * 1024

    def __init__(self, timeouts: Timeouts) -> None:
        self.reader: StreamReader = None
        self.writer: StreamWriter = None
        self.timeouts = timeouts
        self._connected = False

    async def connect(self, request: Request):
        if self._connected is False:
            stream = await asyncio.wait_for(asyncio.open_connection(
                request.url.ip_addr, 
                request.url.port, 
                ssl=request.ssl_context
            ), self.timeouts.connect_timeout)

            self.reader, self.writer = stream

    def write(self, data: bytes):
        self.writer.write(data)

    async def read(self, msg_length: int=READ_NUM_BYTES):
        return await asyncio.wait_for(self.reader.read(msg_length), self.timeouts.total_timeout)
