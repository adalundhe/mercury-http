import asyncio
from typing import AsyncIterator, Dict, Iterator, Union
from urllib.parse import urlencode
from .utils import prepare_chunks, prepare_request_headers


class Connection:

    def __init__(self, dns_address, parsed_url, port, ssl=None) -> None:
        self.dns_address = dns_address
        self.parsed_url = parsed_url
        self.port = port
        self.headers = {}
        self.method = None
        self.is_stream = False
        self.payload = None
        self.ssl = ssl
        self.connected = False
        self._connection = ()

    async def setup_headers(self, method, headers, params):
        self.headers = prepare_request_headers(
            self.parsed_url,
            method,
            {
                **headers,
                **self.headers
            },
            params
        )

        self.method = method

    async def setup_payload(self, payload):

        if payload:

            if isinstance(payload, (AsyncIterator, Iterator)):
                payload = prepare_chunks(payload) 
                self.is_stream = True
                self.headers["Transfer-Encoding"] = "chunked"
                self.payload = payload

            else:
                self.payload: bytes = b""
                content_type = None

                if isinstance(payload, (Dict, tuple)):
                    self.payload = urlencode(payload)
                    content_type = "application/x-www-form-urlencoded"
                else:
                    self.payload = payload
                    content_type = "text/plain"

                if "content-type" not in self.headers or "Content-Type" not in self.headers:
                    self.headers["Content-Type"] = content_type

                if isinstance(self.payload, str):
                    self.payload = self.payload.encode()

                self.headers["Content-Length"] = str(len(self.payload))

    async def connect(self):
        try:
            if self.connected is False:
                self._connection = await asyncio.open_connection(self.dns_address, self.port, ssl=self.ssl)
                self.connected = True

            return self._connection
        except Exception as e:
            return e

