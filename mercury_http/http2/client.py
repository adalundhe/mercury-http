import asyncio
from typing import Awaitable, Dict, Iterator, List, Optional, Set, Tuple, Union
from async_tools.datatypes import AsyncList
from mercury_http.http.client import MercuryHTTPClient
from mercury_http.http.timeouts import Timeouts
from .pool import HTTP2Pool
from .request import HTTP2Request
from .url import URL
from .response import HTTP2Response
from .utils import get_http2_ssl_context


HTTP2ResponseFuture = Awaitable[HTTP2Response]
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
        self.requests: Dict[str, HTTP2Request] = {}
        self.pool: HTTP2Pool = HTTP2Pool(self.concurrency, self.timeouts)
        self.ssl_context = get_http2_ssl_context()
        self.pool.create_pool()

    async def prepare_request(self, 
        request_name: str, 
        url: str, 
        port: int=80, 
        method: str='GET', 
        headers: Dict[str, str]={},
        data: Optional[Union[str, dict, Iterator, bytes, None]]=None, 
        ssl: bool = False,
        user: Optional[str] = None,
        tags: List[Dict[str, str]] = []
    ) -> Awaitable[None]:

        url_config = URL(url, port=port)
        ssl_context = None
        if ssl:
            ssl_context = self.ssl_context
            
        url_config.ssl = ssl_context

        request = HTTP2Request(
            url_config,
            user,
            tags
        )

        if self._hosts.get(url_config.hostname) is None:
            try:
                self._hosts[url_config.hostname] = await asyncio.wait_for(url_config.lookup(), self.timeouts.connect_timeout)
            except Exception as e:
                raise e

        else:
            ip_addr = self._hosts[url_config.hostname]
            url_config.set_ip_addr_and_port(ip_addr)

        request.update(method, headers, data)
        self.requests[request_name] = request

    def update_request(self, request_name: str, method: str, headers: Dict[str, str], payload: Union[str, dict, Iterator, bytes, None]) -> None:
        request = self.requests[request_name]
        request.update(method, headers, payload)
        self.requests[request_name] = request

    async def execute_prepared_request(self, request_name: str) -> HTTP2ResponseFuture:
        request = self.requests[request_name]
        await self.sem.acquire()
        connection = self.pool.connections.pop()

        try:
            await asyncio.wait_for(connection.connect(request_name, request.url), self.timeouts.connect_timeout)
            
            response: HTTP2Response = await asyncio.wait_for(connection.request(request_name, request), self.timeouts.total_timeout)

            self.pool.connections.append(connection)
            self.sem.release()

            return response
        
        except Exception as e:
            return HTTP2Response(
                request_name, 
                request, 
                error=e
            )

    async def execute_prepared_batch(self, request_name: str, concurrency: int=None, timeout: float=None) -> HTTP2BatchResponseFuture:
        try:
            if concurrency is None:
                concurrency = self.concurrency

            if timeout is None:
                timeout = self.timeouts.total_timeout

            return await asyncio.wait([self.execute_prepared_request(request_name) async for _ in AsyncList(range(concurrency))], timeout=timeout)

        except Exception as e:
            return e

    async def request(
        self, 
        request_name: str, 
        url: str, 
        port: int=80, 
        method: str='GET', 
        headers: Dict[str, str]={}, 
        data: Optional[Union[str, dict, Iterator, bytes, None]]=None, 
        ssl: bool = False,
        user: str = Optional[str],
        tags: List[Dict[str, str]] = []
    ) -> HTTP2ResponseFuture:

        if self.requests.get(request_name) is None:
            await self.prepare_request(
                request_name,
                url,
                port,
                method,
                headers,
                data,
                ssl,
                user=user,
                tags=tags
            )

        elif self.hard_cache is False:
            self.update_request(request_name, method, headers, data)

        return await self.execute_prepared_request(request_name)

    async def batch_request(
        self, 
        request_name: str, 
        url: str, 
        port: int=80, 
        method: str='GET', 
        headers: Dict[str, str]={}, 
        data: Optional[Union[str, dict, Iterator, bytes, None]]=None, 
        ssl: bool = False,
        user: str = Optional[str],
        tags: List[Dict[str, str]] = [],
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> HTTP2BatchResponseFuture:

        if self.requests.get(request_name) is None:
            await self.prepare_request(
                request_name,
                url,
                port,
                method,
                headers,
                data,
                ssl,
                user=user,
                tags=tags
            )

        elif self.hard_cache is False:
            self.update_request(request_name, method, headers, data)

        return await self.execute_prepared_batch(request_name, concurrency=concurrency, timeout=timeout)

    async def close(self) -> Awaitable[None]:
        await self.pool.close()