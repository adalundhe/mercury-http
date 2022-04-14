import asyncio
import traceback
from typing import Awaitable, Dict, Optional, Set, Tuple
from async_tools.datatypes import AsyncList
from mercury_http.common.timeouts import Timeouts
from .pool import HTTP2Pool
from mercury_http.common import Request, Response
from .utils import get_http2_ssl_context


HTTP2ResponseFuture = Awaitable[Response]
HTTP2BatchResponseFuture = Awaitable[Tuple[Set[HTTP2ResponseFuture], Set[HTTP2ResponseFuture]]]


class MercuryHTTP2Client:

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = None, hard_cache=False) -> None:

        if timeouts is None:
            timeouts = Timeouts(connect_timeout=10)

        self.concurrency = concurrency
        self.timeouts = timeouts
        self.hard_cache = hard_cache
        self._hosts = {}
        self.requests = {}
        self.sem = asyncio.Semaphore(self.concurrency)
        self.requests: Dict[str, Request] = {}
        self.pool: HTTP2Pool = HTTP2Pool(self.concurrency, self.timeouts)
        self.ssl_context = get_http2_ssl_context()
        self.pool.create_pool()

    async def prepare_request(self, request: Request) -> Awaitable[None]:
        
        if request.url.is_ssl:
            request.ssl_context = self.ssl_context

        if request.is_setup is False:
            await request.setup_http2_request()

            if self._hosts.get(request.url.hostname) is None:
                await request.url.lookup()

        self.requests[request.name] = request

    async def execute_prepared_request(self, request_name: str) -> HTTP2ResponseFuture:
        request = self.requests[request_name]
        response = Response(request)

        await self.sem.acquire()
        try:
            connection = self.pool.connections.pop()

            await asyncio.wait_for(connection.connect(request), self.timeouts.connect_timeout)
            
            response: Response = await asyncio.wait_for(connection.request(request), self.timeouts.total_timeout)

            self.pool.connections.append(connection)
            self.sem.release()

            return response
        
        except Exception as e:
            response.error = e
            return response

    async def request(
        self, 
        request: Request
    ) -> HTTP2ResponseFuture:

        if self.requests.get(request.name) is None:
            await self.prepare_request(request)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http2_request()

        return await self.execute_prepared_request(request.name)

    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> HTTP2BatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http2_request()
        
        return await asyncio.wait([self.execute_prepared_request(request.name) async for _ in AsyncList(range(concurrency))], timeout=timeout)

    async def close(self) -> Awaitable[None]:
        await self.pool.close()