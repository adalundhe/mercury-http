import asyncio
import time
import traceback
from types import FunctionType
from typing import Awaitable, Dict, List, Optional, Set, Tuple
from async_tools.datatypes import AsyncList
from psycopg2 import connect
from mercury_http.common.timeouts import Timeouts
from mercury_http.http2.connection import HTTP2Connection, StreamReset
from mercury_http.http2.stream import AsyncStream
from .pool import HTTP2Pool
from mercury_http.common import Request, Response
from mercury_http.common.ssl import get_http2_ssl_context
from .connection_pool import ConnectionPool


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
        self.requests: Dict[str, Request] = {}
        self.ssl_context = get_http2_ssl_context()
        self.results = []
        self.iters = 0
        self.running = False
        self.pool: HTTP2Pool = HTTP2Pool(self.concurrency, self.timeouts, reset_connections=reset_connections)
        self.pool.create_pool()

        self.connection_pool = ConnectionPool(self.concurrency)
        self.connection_pool.create_pool()
        self.responses = []
        self._connected = False

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

    async def execute_prepared_request(self, request_name: str, idx: int) -> HTTP2ResponseFuture:
        request = self.requests[request_name]
        response = Response(request, type='http2')

        stream_id = idx%self.pool.size

        try:
            connection = self.connection_pool.connections[stream_id]
            await connection.lock.acquire()

            stream = self.pool.connections[stream_id]

            start = time.time()

            await stream.connect(request)

            connection.connect(stream)

            connection.send_request_headers(request, stream)

            await connection.submit_request_body(request, stream)
                
            await connection.receive_response(response, stream)

            elapsed = time.time() - start

            response.time = elapsed
            connection.lock.release()

            return response
            
        except Exception as e:
            response.response_code = 500
            response.error = e
            self.pool.connections[stream_id] = AsyncStream(stream.stream_id, self.timeouts, self.concurrency, self.pool.reset_connections)
            self.connection_pool.connections[stream_id] = HTTP2Connection(stream_id)
            return response

    async def connect_connection(self, request: Request, idx: int):
        try:
            await asyncio.wait_for(self.pool.connections[idx].connect(request), self.timeouts.connect_timeout)
    
        except Exception:
            pass
    
    async def loop_request(self, request: Request, idx: int, atomic_interval: int, timeout: int, stop: int):
        stream_id = idx%self.pool.size
        responses = []
        elapsed = 0
        if self.pool.connections[stream_id].connected is False:
            await self.connect_connection(request, stream_id)
        
        start = time.time()
        while elapsed < timeout:
            responses.append(
                asyncio.create_task(
                    self.execute(request, idx, timeout)
                )
            )
            
            await asyncio.sleep(atomic_interval)
            elapsed = time.time() - start

        self.responses.extend(responses)
        
        return 0

    async def execute(self, request: Request, idx: int, timeout: int):
        try:
            return await asyncio.wait_for(self.execute_prepared_request(request.name, idx), timeout)
        except Exception as e:
            return Response(request, error=e, type='http2')


    async def loop_batch(self, request: Request, atomic_interval: int=1, concurrency: Optional[int]=None, timeout: Optional[float]=None, stop: int=30, checks: Optional[List[FunctionType]]=[]) -> HTTP2BatchResponseFuture:
        if self.running is False:
            self.running = True

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http2_request()

        completed, pending = await asyncio.wait([self.loop_request(request, idx, atomic_interval, timeout, stop) for idx in range(concurrency)], timeout=timeout + stop)

        
        await asyncio.gather(*completed)

        try:
            for pend in pending:
                pend.cancel()
        except Exception:
            pass

        return 0


    async def request(self, request: Request, checks: Optional[List[FunctionType]]=[]) -> HTTP2ResponseFuture:

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http2_request()

        return await self.execute_prepared_request(request.name, 0)

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

        return await asyncio.wait([self.execute_prepared_request(request.name, idx) for idx in range(concurrency)], timeout=timeout)

    async def close(self) -> Awaitable[None]:
        await self.pool.close()