
from ssl import SSLContext
from typing import  Dict, Iterator, Optional, Union
from urllib.parse import urlencode
from .utils import prepare_chunks, prepare_request_headers


class Request:

    def __init__(self, url: str, port: int, method: str, headers: Dict[str, str], params: Dict[str, str], payload: Union[str, dict, Iterator, bytes, None], ssl_context: Optional[SSLContext]) -> None:
        self.url = url
        self.port = port
        self.method = method
        self.headers = headers
        self.params = params
        self.payload = payload
        self.ssl_context = ssl_context
        self.parsed_url = None
        self.host_dns = None
        self.is_stream = False
        self.encoded_headers = None

    def setup(self) -> None:
        self.setup_payload(self.payload)
        self.setup_headers(self.method, self.headers, self.params)

    def update(self, method: str, headers: Dict[str, str], params: Dict[str, str], payload: Union[str, dict, Iterator, bytes, None]) -> None:
        self.setup_payload(payload)
        self.setup_headers(method, headers, params)

    def setup_headers(self, method: str, headers: Dict[str, str], params: Dict[str, str]) -> None:
        self.encoded_headers = prepare_request_headers(
            self.parsed_url,
            method,
            {
                **headers,
                **self.headers
            },
            params
        )

        self.method = method

    def setup_payload(self, payload: Union[str, dict, Iterator, bytes, None]) -> None:

        if payload:

            if isinstance(payload, (Iterator)):
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