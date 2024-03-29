import asyncio
import random
from async_tools.datatypes import AsyncList
from types import FunctionType
from typing import Awaitable, Dict, List, Optional, Set, Tuple, Union
from mercury_http.common import Timeouts
from .context_config import ContextConfig
from .pool import ContextPool
from .command import Command
from .result import Result


PlaywrightResponseFuture = Awaitable[Union[Result, Exception]]
PlaywrightBatchResponseFuture = Awaitable[Tuple[Set[PlaywrightResponseFuture], Set[PlaywrightResponseFuture]]]


class MercuryPlaywrightClient:

    def __init__(self,  concurrency: int = 500, group_size: int=50, timeouts: Timeouts = Timeouts()) -> None:
        self.concurrency = concurrency
        self.pool = ContextPool(concurrency, group_size)
        self.timeouts = timeouts
        self.commands: Dict[str, Command] = {}
        self.sem = asyncio.Semaphore(self.concurrency)
        self.loop = asyncio.get_event_loop()

    async def setup(self, config: ContextConfig):
        self.pool.create_pool(config)
        async for context_group in self.pool:
            await context_group.create()

    async def prepare_command(self, command: Command, checks: List[FunctionType]) -> Awaitable[None]:
        if command.checks is None:
            command.checks = checks
        
        command.options.extra = {
            **command.options.extra,
            'timeout': self.timeouts.total_timeout * 1000
        }

        self.commands[command.name] = command

    async def execute_prepared_command(self, command_name: str) -> PlaywrightResponseFuture:
        command = self.commands[command_name]
        result = Result(command)

        try:
            await self.sem.acquire()

            context = random.choice(self.pool.contexts)
            result = await context.execute(command)

            self.sem.release()

            return result

        except Exception as e:
            result.error = e
            return result

    async def request(self, command: Command, checks: Optional[List[FunctionType]]=[]) -> PlaywrightResponseFuture:

        if self.commands.get(command.name) is None:
            await self.prepare_command(command, checks)

        return await self.execute_prepared_command(command.name)

    async def batch_request(
        self, 
        command: Command,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None,
        checks: Optional[List[FunctionType]]=[]
    ) -> PlaywrightBatchResponseFuture:
    
        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.commands.get(command.name) is None:
            await self.prepare_command(command, checks)

        return await asyncio.wait([self.execute_prepared_command(command.name) async for _ in AsyncList(range(concurrency))], timeout=timeout)

    async def close(self):
        async for context_group in self.pool:
            await context_group.close()