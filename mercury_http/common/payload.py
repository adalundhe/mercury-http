from codecs import StreamWriter
from ctypes import Union
from typing import Dict, Iterator, Union
from urllib.parse import urlencode
from async_tools.datatypes import AsyncList
from .constants import NEW_LINE


class Payload:

    def __init__(self, payload: Union[str, dict, Iterator, bytes, None]) -> None:
        self.data = payload
        self.is_stream = False
        self.size = 0
        self.has_data = payload is not None

    async def __aiter__(self):
        yield self.data

    def __iter__(self):
        for chunk in self.data:
            yield chunk
    
    def setup_payload(self, no_chunking=False) -> None:
        if self.data and no_chunking is False:
            if isinstance(self.data, Iterator):
                chunks = []
                for chunk in self.data:
                    chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                    encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                    self.size += len(encoded_chunk)
                    chunks.append(encoded_chunk)

                self.is_stream = True
                self.data = AsyncList(chunks)

            else:

                if isinstance(self.data, (Dict, tuple)):
                    self.data = urlencode(self.data)

                if isinstance(self.data, str):
                    self.data = self.data.encode()

                self.size = len(self.data)

    async def write_chunks(self, writer: StreamWriter):
        async for chunk in self.data:
            writer.write(chunk)

        writer.write(("0" + NEW_LINE * 2).encode())

