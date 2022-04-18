import asyncio
import time
from types import FunctionType
from typing import Awaitable, Dict, List, Optional, Set, Tuple
from async_tools.datatypes import AsyncList
from mercury_http.common.timeouts import Timeouts
from .pool import HTTP2Pool
from mercury_http.common import Request, Response
from mercury_http.common.ssl import get_http2_ssl_context
from .connection import HTTP2Connection


HTTP2ResponseFuture = Awaitable[Response]
HTTP2BatchResponseFuture = Awaitable[Tuple[Set[HTTP2ResponseFuture], Set[HTTP2ResponseFuture]]]


class MercuryHTTP2Client:

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = None, hard_cache: bool=False, reset_connections: bool=False) -> None:

        if timeouts is None:
            timeouts = Timeouts(connect_timeout=10)

        self.concurrency = concurrency
        self.timeouts = timeouts
        self.hard_cache = hard_cache
        self._hosts = {}
        self.requests = {}
        self.sem = asyncio.Semaphore(self.concurrency)
        self.requests: Dict[str, Request] = {}
        self.pool: HTTP2Pool = HTTP2Pool(self.concurrency, self.timeouts, reset_connections=reset_connections)
        self.ssl_context = get_http2_ssl_context()
        self.pool.create_pool()

    async def prepare_request(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context

            if request.is_setup is False:
                request.setup_http2_request()

                if self._hosts.get(request.url.hostname) is None:
                    self._hosts[request.url.hostname] = await request.url.lookup()
                else:
                    request.url.ip_addr = self._hosts[request.url.hostname]

                if request.checks is None:
                    request.checks = checks

            self.requests[request.name] = request

        except Exception as e:
            return Response(request, error=e, type='http2')

    async def execute_prepared_request(self, request_name: str) -> HTTP2ResponseFuture:
        request = self.requests[request_name]
        response = Response(request, type='http2')

        await self.sem.acquire()

        try:
            connection = self.pool.connections.pop()
            start = time.time()

            await connection.connect(request)
            stream_id = connection.get_stream_id()
            
            connection.send_request_headers(stream_id, request)

            if request.payload.has_data:
                await connection.submit_request_body(stream_id, request)
                
            await connection.receive_response(stream_id, response)

            elapsed = time.time() - start

            response.time = elapsed
     
            self.pool.connections.append(connection)
            self.sem.release()

            return response
        
        except Exception as e:
            response.error = e
            self.pool.connections.append(
                HTTP2Connection(reset_connection=self.pool.reset_connections)
            )

            self.sem.release()
            return response

    async def request(self, request: Request, checks: Optional[List[FunctionType]]=[]) -> HTTP2ResponseFuture:

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http2_request()

        return await self.execute_prepared_request(request.name)

    async def batch_request(self, request: Request, concurrency: Optional[int]=None, timeout: Optional[float]=None, checks: Optional[List[FunctionType]]=[]) -> HTTP2BatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http2_request()
        
        return await asyncio.wait([self.execute_prepared_request(request.name) async for _ in AsyncList(range(concurrency))], timeout=timeout)

    async def close(self) -> Awaitable[None]:
        await self.pool.close()