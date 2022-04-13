import aiodns
import asyncio
from asyncio import AbstractEventLoop
from urllib.parse import urlparse


class URL:

    def __init__(self, url: str, port: int=80, ssl=False, loop: AbstractEventLoop=asyncio.get_event_loop()) -> None:
        self.resolver = aiodns.DNSResolver(loop=loop)
        self.parsed = urlparse(url)
        self.port = port
        self.ip_addr = None
        self.ssl = ssl
        self.full = url

    async def lookup(self):
        host = await self.resolver.gethostbyname(self.parsed.hostname, 0)
        self.ip_addr = host.addresses.pop()

        if self.ssl:
            self.port = 443

    @property
    def scheme(self):
        return self.parsed.scheme

    @property
    def hostname(self):
        return self.parsed.hostname

    @property
    def path(self):
        url_path = self.parsed.path
        if len(self.parsed.query) > 0:
            url_path += f'?{self.parsed.query}'

        return url_path
    @property
    def query(self):
        return self.parsed.query

    @property
    def authority(self):
        return self.parsed.hostname