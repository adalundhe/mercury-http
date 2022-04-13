from __future__ import annotations
from typing import Dict, Iterator, Union
from .params import Params
from .metadata import Metadata
from .url import URL
from .payload import Payload
from .headers import Headers

class Request:

    def __init__(self, name: str, url: str, method: str = 'GET', headers: Dict[str, str]={}, params: Dict[str, str]={}, payload: Union[str, dict, Iterator, bytes, None]=None) -> None:
        self.name = name
        self.method = method
        self.url = URL(url)
        self.params = Params(params)
        self.headers = Headers(headers)
        self.payload = Payload(payload)
        self.metadata = Metadata()
        self.ssl_context = None

    def __aiter__(self):
        return self.payload.__aiter__()

    def update(self, request: Request):
        self.method = request.method
        self.headers.data = request.headers.data
        self.payload.data = request.payload.data
        self.params.data = request.params.data

    def setup_http_request(self):
        self.payload.setup_payload()

        if self.payload.has_data:
            self.headers['Content-Length'] = str(self.payload.size)

        self.headers.setup_http_headers(self.method, self.url, self.params)


    def setup_http2_request(self):
        self.payload.setup_payload()

        if self.payload.has_data:
            self.headers['Content-Length'] = str(self.payload.size)
 
        self.headers.setup_http2_headers(self.method, self.url)

    def setup_websocket_request(self):
        self.payload.setup_payload()
        
        if self.payload.has_data:
            self.headers['Content-Length'] = str(self.payload.size)

        self.headers.setup_websocket_headders(self.method, self.url)