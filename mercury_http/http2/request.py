from typing import Dict, Union, Iterator, Optional, List
from urllib.parse import urlencode
from .url import URL


class HTTP2Request:

    def __init__(self, url: URL, user: Optional[str], tags: List[Dict[str, str]]) -> None:
        self.method = None
        self.url = url
        self.headers = {}
        self.payload = None
        self.user = user
        self.tags = tags

    def __aiter__(self):
        yield self.payload

    def update(self, method: str, headers: Dict[str, str], payload: Union[str, dict, Iterator, bytes, None]) -> None:
        self.method = method
        self.setup_payload(payload)
        self.setup_headers(headers)

    def setup_headers(self, headers: Dict[str, str]) -> None:

        self.headers = [
                (b":method", self.method),
                (b":authority", self.url.authority),
                (b":scheme", self.url.scheme),
                (b":path", self.url.path),
            ] + [
                (k.lower(), v)
                for k, v in headers.items()
                if k.lower()
                not in (
                    b"host",
                    b"transfer-encoding",
                )
            ]

    def setup_payload(self, payload: Union[str, dict, Iterator, bytes, None]) -> None:

        if payload:
            self.payload: bytes = b""

            if isinstance(payload, (Dict, tuple, list)):
                payload = urlencode(payload)
            
            if isinstance(self.payload, str):
                payload = payload.encode()

            self.payload = payload