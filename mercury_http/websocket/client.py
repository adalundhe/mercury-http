import time
import asyncio
import traceback
from typing import Awaitable, Optional, Union, Tuple, Set
from async_tools.datatypes import AsyncList
from mercury_http.http.client import MercuryHTTPClient
from mercury_http.common.response import Response, Request
from mercury_http.common.timeouts import Timeouts
from .utils import get_header_bits, get_message_buffer_size, websocket_headers_to_iterator


WebsocketResponseFuture = Awaitable[Union[Response, Exception]]
WebsocketBatchResponseFuture = Awaitable[Tuple[Set[WebsocketResponseFuture], Set[WebsocketResponseFuture]]]


class MercuryWebsocketClient(MercuryHTTPClient):


    def __init__(self, concurrency: int = 10 ** 3, timeouts: Timeouts = Timeouts(), hard_cache=False) -> None:
        super(
            MercuryWebsocketClient,
            self
        ).__init__(concurrency, timeouts, hard_cache)

    async def execute_prepared_request(self, request_name: str) -> WebsocketResponseFuture:

        request = self.requests[request_name]
        response = Response(request)

        await self.sem.acquire() 

        try:
            connection = self.pool.connections.pop()
            start = time.time()
            stream = await asyncio.wait_for(connection.connect(
                request_name,
                request.url.ip_addr,
                request.url.port,
                ssl=request.ssl_context
            ), timeout=self.timeouts.connect_timeout)
            if isinstance(stream, Exception):
                response.error = stream
                return response

            reader, writer = stream
            writer.write(request.headers.encoded_headers)
            
            if request.payload.data:
                if request.payload.is_stream:
                    await request.payload.write_chunks(writer)

                else:
                    writer.write(request.payload.data)

            line = await asyncio.wait_for(reader.readuntil(), self.timeouts.socket_read_timeout)

            response.response_code = line
            raw_headers = b''
            async for key, value, header_line in websocket_headers_to_iterator(reader):
                response.headers[key] = value
                raw_headers += header_line

            if request.payload.has_data:
                header_bits = get_header_bits(raw_headers)
                header_content_length = get_message_buffer_size(header_bits)
                response.body = await reader.read(min(16384, header_content_length))
            
            elapsed = time.time() - start

            response.time = elapsed
            
            self.pool.connections.append(connection)
            self.sem.release()

            return response

        except Exception as e:
            response.error = e
            return response

    async def request(
        self, 
        request: Request
    ) -> WebsocketBatchResponseFuture:

        if self.requests.get(request.name) is None:

            await self.prepare_request(request)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_websocket_request()

        return await self.execute_prepared_request(request.name)
        
    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> WebsocketBatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_websocket_request()
        
        return await asyncio.wait([self.execute_prepared_request(request.name) async for _ in AsyncList(range(concurrency))], timeout=timeout)