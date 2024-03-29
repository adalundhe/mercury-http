from .context_group import ContextGroup
from .context_config import ContextConfig


class ContextPool:

    def __init__(self, pool_size, group_size) -> None:
        self.size = pool_size
        self.group_size = group_size
        self.groups_count = int(pool_size/group_size)
        self.contexts = []

    async def __aiter__(self):
        for context_group in self.contexts:
            yield context_group

    def create_pool(self, config: ContextConfig):
        self.contexts = [
            ContextGroup(
                **config.data,
                concurrency=self.group_size
            ) for _ in range(self.groups_count)
        ]