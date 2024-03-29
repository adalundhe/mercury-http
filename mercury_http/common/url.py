from typing import Dict
import aiodns
from urllib.parse import urlparse
from .params import Params


class URL:

    def __init__(self, url: str, port: int=80) -> None:
        self.resolver = aiodns.DNSResolver()
        self.parsed = urlparse(url)
        self.port = self.parsed.port if self.parsed.port else port
        self.ip_addr = None
        self.is_ssl = 'https' in url or 'wss' in url
        self.full = url

    async def lookup(self):
        hosts = await self.resolver.query(self.parsed.hostname, 'A')
        resolved = hosts.pop()
        self.ip_addr = resolved.host

        if self.is_ssl:
            self.port = 443

        return self.ip_addr

    def set_ip_addr_and_port(self, ip_addr):
        self.ip_addr = ip_addr
        
        if self.is_ssl:
            self.port = 443

    @property
    def params(self):
        return self.parsed.params

    @property
    def scheme(self):
        return self.parsed.scheme

    @property
    def hostname(self):
        return self.parsed.hostname

    @property
    def path(self):
        url_path = self.parsed.path

        if len(url_path) == 0:
            url_path = "/"

        if len(self.parsed.query) > 0:
            url_path += f'?{self.parsed.query}'

        return url_path
    @property
    def query(self):
        return self.parsed.query

    @property
    def authority(self):
        return self.parsed.hostname