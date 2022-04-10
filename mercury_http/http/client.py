import ssl
import asyncio
import traceback
import aiodns
import time
import random
from typing import AsyncIterator, Iterator, List
from typing import Dict
from urllib.parse import urlparse
from async_tools.datatypes import AsyncList
from .response import Response
from .connection import Connection
from .timeouts import Timeouts
from .utils import prepare_request_headers, write_chunks




class MercuryHTTPClient:

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = Timeouts()) -> None:
        self.concurrency = concurrency
        self.connections: Dict[str, List[Connection]] = {}
        self.timeouts = timeouts
        self.sem = asyncio.Semaphore(self.concurrency)
        self.loop = asyncio.get_event_loop()
        self.resolver = aiodns.DNSResolver(loop=self.loop)


        self.ssl_context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
        )
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE

    
    async def query(self,name, query_type):
        return await self.resolver.query(name, query_type)

    async def parse_headers_iterator(self, reader):
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

    async def prepare(self, request_name, url, port=80, method='GET', headers={}, params=None, data=None, ssl=False):
        parsed_url = urlparse(url)
        result = await self.query(parsed_url.hostname, 'A')
        ares_host = random.choice(result)
 
        stream_batches = [100 for _ in range(int(self.concurrency/100))]
        last_batch_size = 100 + (self.concurrency%100)
        stream_batches[len(stream_batches) - 1] = last_batch_size

        ssl_context = None

        if ssl:
            port = 443
            ssl_context = self.ssl_context

        self.connections[request_name] = []
        for _ in range(self.concurrency):
            connection = Connection(ares_host.host, parsed_url, port, ssl=ssl_context) 
       
            await connection.setup_payload(data)
            await connection.setup_headers(method, headers, params)

            self.connections[request_name].append(connection)

    async def batch_request(self, request_name, concurrency=None):
        try:
            if concurrency is None:
                concurrency = self.concurrency

            return await asyncio.wait([self.execute_request(request_name) async for _ in AsyncList(range(concurrency))], timeout=self.timeouts.total_timeout)

        except Exception as e:
            return e

    async def execute_request(self, request_name):

        await self.sem.acquire()

        connections: List[Connection] = self.connections[request_name]      
        try:
            start = time.time()
            connection = connections.pop()
            stream = await asyncio.wait_for(connection.connect(), timeout=self.timeouts.connect_timeout)
            if isinstance(stream, Exception):
                return Response(error=stream)

            reader, writer = stream
            writer.write(connection.headers)
            
            if connection.payload:
                if connection.is_stream:
                    await write_chunks(connection, connection.payload)

                else:
                    writer.write(connection.payload)

            line = await asyncio.wait_for(reader.readuntil(), self.timeouts.socket_read_timeout)

            headers = {}
            async for key, value in self.parse_headers_iterator(reader):
                headers[key] = value
 
            response = Response(headers=headers, status_string=line)
            if response.size:
                response.body = await asyncio.wait_for(reader.readexactly(response.size), self.timeouts.total_timeout)

            elapsed = time.time() - start
            response.time = elapsed
            
            self.connections[request_name].append(connection)
            self.sem.release()
            return response

        except Exception as e:
            return Response(error=e)

