import json
from gzip import decompress as gzip_decompress
from zlib import decompress as zlib_decompress

class Response:

    def __init__(self, headers: dict = {}, status_string: str = None, error: Exception = None) -> None:
        
        self.chunked = headers.get("transfer-encoding", "") == "chunked"
        self.keepalive = "close" not in headers.get("connection", "")
        self.compressed = headers.get("content-encoding", "")
        self.content_type = headers.get("content-type", "")
        size = int(headers.get("content-length", 0))

        self.size = size
        self.headers = headers
        self.body = b''
        self.error = error
        self.time = 0

    @property
    async def data(self):
        data = self.body
        if self.compressed == "gzip":
            data = gzip_decompress(self.body)
        elif self.compressed == "deflate":
            data = zlib_decompress(self.body)

        if self.content_type == "application/json":
            data = json.loads(self.body)

        return data
        
