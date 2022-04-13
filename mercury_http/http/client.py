import ssl
import asyncio
import aiodns
import time
import random
from typing import Any, Awaitable, Dict, Iterator, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse
from async_tools.datatypes import AsyncList
from asyncio import StreamReader
from .request import Request
from .response import Response
from .pool import Pool
from .timeouts import Timeouts
from .utils import write_chunks


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

    
    async def query(self, name: str, query_type: str) -> Awaitable[Any]:
        return await self.resolver.query(name, query_type)

    async def parse_headers_iterator(self, reader: StreamReader) -> Awaitable[Tuple[str, str]]:
        """Transform loop to iterator."""
        while True:
            # StreamReader already buffers data reading so it is efficient.
            res_data = await reader.readline()
            if b": " not in res_data and b":" not in res_data:
                break

            decoded = res_data.rstrip().decode()
            pair = decoded.split(": ", 1)
            if len(pair) < 2:
                pair = decoded.split(":")

            key, value = pair
            yield key.lower(), value
    
    async def read_body(self, reader: StreamReader, response: Response) -> Awaitable[Response]:
        while True:
            chunk = await reader.readline()
            if chunk == b'0\r\n' or chunk is None:
                response.body += b'\r\n'
                break
            response.body += chunk

        return response

    async def prepare_request(
        self, 
        request_name: str, 
        url: str, 
        port: int=80, 
        method: str='GET', 
        headers: Dict[str, str]={},
        params: Optional[Dict[str, str]]=None, 
        data: Optional[Union[str, dict, Iterator, bytes, None]]=None, 
        ssl: bool=False,
        user: Optional[str] = None,
        tags: List[Dict[str, str]] = []
    ) -> Awaitable[None]:
        
        ssl_context = None
        if ssl:
            port = 443
            ssl_context = self.ssl_context

        request = Request(
            url,
            port,
            method,
            headers,
            params,
            data,
            ssl_context,
            user,
            tags
        )

        parsed_url = urlparse(url)

        request.parsed_url = parsed_url

        hostname = parsed_url.hostname
        if self._hosts.get(hostname) is None:
            hosts = await self.query(hostname, 'A')
            ares_host = random.choice(hosts)
            request.host_dns = ares_host.host

        
        request.setup()

        self.requests[request_name] = request

    def update_request(self, request_name: str, method: str, headers: Dict[str, str], params: Union[Dict[str, str], None], payload: Union[str, dict, Iterator, bytes, None]):
        request = self.requests[request_name]
        request.update(method, headers, params, payload)
        self.requests[request_name] = request

    async def execute_prepared_request(self, request_name: str) -> HTTPResponseFuture:

        await self.sem.acquire() 
        request = self.requests[request_name]
        try:
            connection = self.pool.connections.pop()
            start = time.time()
            stream = await asyncio.wait_for(connection.connect(
                request_name,
                request.host_dns,
                request.port,
                ssl=request.ssl_context
            ), timeout=self.timeouts.connect_timeout)
            if isinstance(stream, Exception):
                return Response(error=stream)

            reader, writer = stream
            writer.write(request.encoded_headers)
            
            if request.payload:
                if request.is_stream:
                    await write_chunks(writer, request.payload)

                else:
                    writer.write(request.payload)

            line = await asyncio.wait_for(reader.readuntil(), self.timeouts.socket_read_timeout)

            headers = {}
            async for key, value in self.parse_headers_iterator(reader):
                headers[key] = value
            
            response = Response(
                request_name,
                request.url,
                request.parsed_url,
                request.method,
                headers=headers, 
                status_string=line
            )
            if response.size:
                response.body = await asyncio.wait_for(reader.readexactly(response.size), self.timeouts.total_timeout)
            else:
                response = await asyncio.wait_for(self.read_body(reader, response), self.timeouts.total_timeout)    
            
            elapsed = time.time() - start
            response.time = elapsed
            
            self.pool.connections.append(connection)
            self.sem.release()
            return response

        except Exception as e:
            return Response(
                request_name,
                request.url,
                request.parsed_url,
                request.method,
                error=e
            )

    async def execute_prepared_batch(self, request_name: str, concurrency: int=None, timeout: float=None) -> HTTPBatchResponseFuture:
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
        params: Optional[Dict[str, str]]=None, 
        data: Optional[Union[str, dict, Iterator, bytes, None]]=None, 
        ssl: bool=False,
        user: str = Optional[str],
        tags: List[Dict[str, str]] = []
    ) -> HTTPResponseFuture:

        if self.requests.get(request_name) is None:
            await self.prepare_request(
                request_name,
                url,
                port,
                method,
                headers,
                params,
                data,
                ssl=ssl,
                user=user,
                tags=tags
            )

        elif self.hard_cache is False:
            self.update_request(request_name, method, headers, params, data)

        return await self.execute_prepared_request(request_name)
        

    async def batch_request(
        self, 
        request_name: str, 
        url: str, 
        port: int=80, 
        method: str='GET', 
        headers: Dict[str, str]={}, 
        params: Optional[Dict[str, str]]=None, 
        data: Optional[Union[str, dict, Iterator, bytes, None]]=None, 
        ssl: bool=False,
        user: str = Optional[str],
        tags: List[Dict[str, str]] = [],
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> HTTPBatchResponseFuture:

        if self.requests.get(request_name) is None:
            await self.prepare_request(
                request_name,
                url,
                port,
                method,
                headers,
                params,
                data,
                ssl=ssl,
                user=user,
                tags=tags
            )

        elif self.hard_cache is False:
            self.update_request(request_name, method, headers, params, data)

        return await self.execute_prepared_batch(request_name, concurrency=concurrency, timeout=timeout)
