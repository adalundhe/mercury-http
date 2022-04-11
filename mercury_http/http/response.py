import json
from gzip import decompress as gzip_decompress
from urllib.parse import ParseResult
from zlib import decompress as zlib_decompress
from typing import Dict, List, Optional, Union


class Response:

    def __init__(self, name: str, url: str, parsed_url: ParseResult, method: str, headers: dict = {}, status_string: str = None, error: Exception = None, user: Optional[str] = None, tags: List[Dict[str, str]] = []) -> None:
        self.name = name
        self.url = url
        self.method = method
        self.path = parsed_url.path
        self.hostname = parsed_url.hostname
        self.chunked = headers.get("transfer-encoding", "") == "chunked"
        self.keepalive = "close" not in headers.get("connection", "")
        self.compressed = headers.get("content-encoding", "")
        self.content_type = headers.get("content-type", "")
        size = int(headers.get("content-length", 0))
        self.status_string = status_string
        self.size = size
        self.headers = headers
        self.body = b''
        self.error = error
        self.time = 0
        self.user = user
        self.tags = tags

    @property
    def data(self) -> Union[str, dict, None]:
        data = self.body
        if self.compressed == "gzip":
            data = gzip_decompress(self.body)
        elif self.compressed == "deflate":
            data = zlib_decompress(self.body)

        if self.content_type == "application/json":
            data = json.loads(self.body)
        
        elif isinstance(self.body, bytes):
            data = data.decode()

        return data

    @property
    def version(self) -> Union[str, None]:
        try:
            status_string = self.status_string.split()
            return status_string[0].decode()
        except Exception:
            return None

    @property
    def status(self) -> Union[int, None]:
        try:
            status_string = self.status_string.split()
            return int(status_string[1])
        except Exception:
            return None

    @property
    def reason(self) -> Union[str, None]:
        try:
            status_string = self.status_string.split()
            return status_string[2].decode()
        except Exception:
            return None
        
