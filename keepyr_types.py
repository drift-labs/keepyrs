from abc import ABC, abstractmethod
from typing import Optional


class Bot(ABC):
    @abstractmethod
    def __init__(self, name: str, dry_run: bool, default_interval_ms: Optional[int]):
        self.name = name
        self.dry_run = dry_run
        self.default_interval_ms = default_interval_ms

    @abstractmethod
    async def init(self):
        pass

    @abstractmethod
    async def reset(self):
        pass

    @abstractmethod
    async def start_interval_loop(self, interval_ms: Optional[int]):
        pass

    @abstractmethod
    async def health_check(self):
        pass
