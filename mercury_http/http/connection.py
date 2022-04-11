import asyncio
from asyncio import StreamReader, StreamWriter
from ssl import SSLContext
from typing import Optional, Tuple, Union


class Connection:

    def __init__(self) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.connected = False
        self._connection: Tuple[StreamReader, StreamWriter] = ()

    def setup(self, dns_address: str, port: int, ssl: Union[SSLContext, None]) -> None:
        self.dns_address = dns_address
        self.port = port
        self.ssl = ssl

    async def connect(self, dns_address: str, port: int, ssl: Optional[SSLContext]=None) -> Union[Tuple[StreamReader, StreamWriter], Exception]:
        try:
            if self.connected is False:
                self._connection = await asyncio.open_connection(dns_address, port, ssl=ssl)
                self.connected = True

                self.dns_address = dns_address
                self.port = port
                self.ssl = ssl

            return self._connection
        except Exception as e:
            return e

