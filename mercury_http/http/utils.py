
from codecs import StreamWriter
from typing import AsyncIterator, Iterator, Union
from typing import Dict, Sequence, Tuple
from urllib.parse import urlencode, ParseResult
from async_tools.datatypes import AsyncList



HeadersType = Dict[str, str]

_NEW_LINE = '\r\n'


def add_headers(headers: HeadersType, headers_to_add: HeadersType):
    """Safe add multiple headers."""

    for key, data in headers_to_add.items():
        headers[key] = data


def prepare_request_headers(
    url: ParseResult,
    # connection: Any,
    method: str,
    headers: Dict[str, str] = {},
    params: Union[
        Dict[str, str],
        Sequence[Tuple[str, str]],
    ] = None,
    multipart: bool = None,
) -> Union[bytes, Dict[str, str]]:
        path = url.path
        has_query = False
        
        if url.query:
            has_query = True
            path = f'{path}?{url.query}'

        if params:
            params = urlencode(params)

            if has_query:
                path = f'{path}{params}'
            
            else:
                path = f'{path}?{params}'

        get_base = f"{method.upper()} {path} HTTP/1.1{_NEW_LINE}"
        port = url.port or (443 if url.scheme == "https" else 80)
        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f'{hostname}:{port}'

        
        headers_base = {}

        add_headers(headers_base, {
            "HOST": hostname,
            "Connection": "keep-alive",
            "User-Agent": "mercury-http",
            **headers
        })



        for key, value in headers_base.items():
            get_base += f"{key}: {value}{_NEW_LINE}"

        return (get_base + _NEW_LINE).encode()


async def prepare_chunks(body: Union[AsyncIterator, Iterator]):
    """Send chunks."""
    chunks = []
    if isinstance(body, AsyncIterator):
        async for chunk in body:
            chunk_size = hex(len(chunk)).replace("0x", "") + _NEW_LINE
            chunks.append(chunk_size.encode() + chunk + _NEW_LINE.encode())

    elif isinstance(body, Iterator):
        for chunk in body:
            chunk_size = hex(len(chunk)).replace("0x", "") + _NEW_LINE
            chunks.append(chunk_size.encode() + chunk + _NEW_LINE.encode())

    return AsyncList(chunks)


async def write_chunks(writer: StreamWriter, body: AsyncList):
    async for chunk in body:
        writer.write(chunk)

    writer.write(("0" + _NEW_LINE * 2).encode())
