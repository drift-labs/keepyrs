from abc import ABC, abstractmethod
from typing import Optional
from dataclasses import dataclass
from driftpy.types import MarketType


class Bot(ABC):
    @abstractmethod
    async def init(self):
        pass

    @abstractmethod
    async def reset(self):
        pass

    @abstractmethod
    async def start_interval_loop(self, interval_ms: Optional[int] = 1000):
        pass

    @abstractmethod
    async def health_check(self):
        pass


@dataclass
class BotConfig:
    bot_id: str
    dry_run: bool


@dataclass
class JitMakerConfig(BotConfig):
    market_indexes: Optional[list[int]]
    sub_accounts: Optional[list[int]]
    market_type: MarketType
