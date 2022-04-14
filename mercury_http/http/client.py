import ssl
import asyncio
import traceback
import aiodns
import time
from typing import Any, Awaitable, Dict, Optional, Set, Tuple, Union
from async_tools.datatypes import AsyncList
from mercury_http.common import Request
from mercury_http.common import Response
from .pool import Pool
from mercury_http.common.timeouts import Timeouts
from .utils import (
    http_headers_to_iterator, 
    read_body
)


HTTPResponseFuture = Awaitable[Union[Response, Exception]]
HTTPBatchResponseFuture = Awaitable[Tuple[Set[HTTPResponseFuture], Set[HTTPResponseFuture]]]


class MercuryHTTPClient:

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = Timeouts(), hard_cache=False) -> None:
        self.concurrency = concurrency
        self.pool = Pool(concurrency)
        self.requests: Dict[str, Request] = {}
        self._hosts = {}
        self._parsed_urls = {}
        self.timeouts = timeouts
        self.sem = asyncio.Semaphore(self.concurrency)
        self.loop = asyncio.get_event_loop()
        self.resolver = aiodns.DNSResolver(loop=self.loop)
        self.hard_cache = hard_cache

        self.ssl_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
        )
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

        self.pool.create_pool()
    
    async def prepare_request(self, request: Request) -> Awaitable[None]:
        
        if request.url.is_ssl:
            request.ssl_context = self.ssl_context

        if request.is_setup is False:
            request.setup_http_request()

            if self._hosts.get(request.url.hostname) is None:
                self._hosts[request.url.hostname] = await request.url.lookup()

        self.requests[request.name] = request

    async def execute_prepared_request(self, request_name: str) -> HTTPResponseFuture:

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
            
            if request.payload.has_data:
                if request.payload.is_stream:
                    await request.payload.write_chunks(writer)

                else:
                    writer.write(request.payload.data)

            line = await asyncio.wait_for(reader.readuntil(), self.timeouts.socket_read_timeout)
            response.response_code = line

            async for key, value in http_headers_to_iterator(reader):
                response.headers[key] = value
            
            if response.size:
                response.body = await asyncio.wait_for(reader.readexactly(response.size), self.timeouts.total_timeout)
            else:
                response = await asyncio.wait_for(read_body(reader, response), self.timeouts.total_timeout)    
            
            elapsed = time.time() - start

            response.time = elapsed
            
            self.pool.connections.append(connection)
            self.sem.release()
            return response

        except Exception as e:
            response.error = str(e)
            return response

    async def request(
        self, 
        request: Request
    ) -> HTTPResponseFuture:

        if self.requests.get(request.name) is None:
            await self.prepare_request(request)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http_request()

        return await self.execute_prepared_request(request.name)
        
    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> HTTPBatchResponseFuture:
    
        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http_request()

        return await asyncio.wait([self.execute_prepared_request(request.name) async for _ in AsyncList(range(concurrency))], timeout=timeout)
