from abc import ABC, abstractmethod
from typing import Optional, Union
from dataclasses import dataclass

from driftpy.types import MarketType
from driftpy.drift_client import DriftClient
from driftpy.user_map.user_map import UserMap
from driftpy.constants.config import DriftEnv

from jit_proxy.jitter.jitter_shotgun import JitterShotgun  # type: ignore
from jit_proxy.jitter.jitter_sniper import JitterSniper  # type: ignore
from driftpy.dlob.dlob_node import DLOBNode

MakerNodeMap = dict[str, list[DLOBNode]]


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


@dataclass
class JitMakerConfig(BotConfig):
    market_indexes: list[int]
    sub_accounts: list[int]
    market_type: MarketType
    drift_client: DriftClient
    usermap: UserMap
    jitter: Union[JitterSniper, JitterShotgun]
    drift_env: DriftEnv
    target_leverage: float = 1.0
    spread: float = 0.0


@dataclass
class PerpFillerConfig(BotConfig):
    filler_polling_interval: Optional[float] = None
    revert_on_failure: bool = False
    simulate_tx_for_cu_estimate: bool = False
    use_burst_cu_limit: bool = False
