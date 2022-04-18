import asyncio
import weakref
import socket
from asyncio import StreamReader, StreamWriter
from mercury_http.common import Timeouts, Request


class TLSStreamReaderProtocol(asyncio.StreamReaderProtocol):

    def upgrade_reader(self, reader: StreamReader):
        if self._stream_reader is not None:
            self._stream_reader.set_exception(Exception('upgraded connection to TLS, this reader is obsolete now.'))
        self._stream_reader_wr = weakref.ref(reader)
        self._source_traceback = reader._source_traceback


async def open_tls_stream(request: Request):
    # this does the same as loop.open_connection(), but TLS upgrade is done
    # manually after connection be established.
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(limit=2**64, loop=loop)
    protocol = TLSStreamReaderProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(
        lambda: protocol, request.url.ip_addr, request.url.port, family=socket.AF_INET
    )
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    # here you can use reader and writer for whatever you want, for example
    # start a proxy connection and start TLS to target host later...
    # now perform TLS upgrade
    if request.ssl_context:
        transport = await loop.start_tls(
            transport,
            protocol,
            sslcontext=request.ssl_context,
            server_side=False,
            server_hostname=request.url.hostname
        )

        reader = asyncio.StreamReader(limit=2**64, loop=loop)
        protocol.upgrade_reader(reader) # update reader
        protocol.connection_made(transport) # update transport
        writer = asyncio.StreamWriter(transport, protocol, reader, loop) # update writer
    return reader, writer


class AsyncStream:
    READ_NUM_BYTES = 65536

    def __init__(self, timeouts: Timeouts) -> None:
        self.reader: StreamReader = None
        self.writer: StreamWriter = None
        self.timeouts = timeouts
        self._connected = False

    async def connect(self, request: Request):
        if self._connected is False:
            stream = await asyncio.wait_for(open_tls_stream(request), self.timeouts.connect_timeout)
            
            self.reader, self.writer = stream

    def write(self, data: bytes):
        self.writer.write(data)

    async def read(self, msg_length: int=READ_NUM_BYTES):
        return await asyncio.wait_for(self.reader.read(msg_length), self.timeouts.total_timeout)
