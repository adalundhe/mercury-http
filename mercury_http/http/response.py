import json
from gzip import decompress as gzip_decompress
from zlib import decompress as zlib_decompress
from typing import Union


class Response:

    def __init__(self, headers: dict = {}, status_string: str = None, error: Exception = None) -> None:
        
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

        self._task = None

    @property
    def data(self) -> Union[str, dict, None]:
        data = self.body
        if self.compressed == "gzip":
            data = gzip_decompress(self.body)
        elif self.compressed == "deflate":
            data = zlib_decompress(self.body)

        if self.content_type == "application/json":
            data = json.loads(self.body)
        
        if isinstance(self.body, bytes):
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
        
