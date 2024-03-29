import time


import asyncio
import traceback
from types import FunctionType
from mercury_http import http2
from mercury_http.http2 import MercuryHTTP2Client
from mercury_http.http import MercuryHTTPClient
from mercury_http.common import Timeouts
from mercury_http.common import Request, Response
from typing import Awaitable, List, Union, Tuple, Set, Optional
from async_tools.datatypes import AsyncList


GraphQLResponseFuture = Awaitable[Union[Response, Exception]]
GraphQLBatchResponseFuture = Awaitable[Tuple[Set[GraphQLResponseFuture], Set[GraphQLResponseFuture]]]


class MercuryGraphQLClient:

    def __init__(self, concurrency: int = 10 ** 3, timeouts: Timeouts = Timeouts(), hard_cache=False, use_http2=False, reset_connection: bool=False) -> None:
        
        if use_http2:
            self._client = MercuryHTTP2Client(
                concurrency=concurrency,
                timeouts=timeouts,
                hard_cache=hard_cache,
                reset_connections=reset_connection
            )
        
        else:
            self._client = MercuryHTTPClient(
                concurrency=concurrency,
                timeouts=timeouts,
                hard_cache=hard_cache,
                reset_connections=reset_connection
            )
        
        self._use_http2 = use_http2

    async def prepare_request(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self._client.ssl_context

            if request.is_setup is False:
                request.setup_graphql_request(use_http2=self._use_http2)

                if self._client._hosts.get(request.url.hostname) is None:
                    self._client._hosts[request.url.hostname] = await request.url.lookup()
                else:
                    request.url.ip_addr = self._client._hosts[request.url.hostname]

                if request.checks is None:
                    request.checks = checks

            self._client.requests[request.name] = request
        
        except Exception as e:
            return Response(request, error=e, type='graphql')

    async def execute_prepared_request(self, request_name):
        response: Response = await self._client.execute_prepared_request(request_name)
        response.type = 'graphql'

        return response

    async def request(self, request: Request, checks: List[FunctionType]=[]) -> GraphQLResponseFuture:

        if self._client.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self._client.hard_cache is False:
            self._client.requests[request.name].update(request)
            self._client.requests[request.name].setup_graphql_request(use_http2=self._use_http2)

        return await self.execute_prepared_request(request.name)
        
    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None,
        checks: Optional[List[FunctionType]]=[]
    ) -> GraphQLBatchResponseFuture:
    
        if concurrency is None:
            concurrency = self._client.concurrency

        if timeout is None:
            timeout = self._client.timeouts.total_timeout

        if self._client.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self._client.hard_cache is False:
            self._client.requests[request.name].update(request)
            self._client.requests[request.name].setup_graphql_request(use_http2=self._use_http2)

        return await asyncio.wait([self.execute_prepared_request(request.name) async for _ in AsyncList(range(concurrency))], timeout=timeout)
