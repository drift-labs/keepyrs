import asyncio
import os
import logging
import time

from datetime import datetime
from typing import Optional, Union
from dotenv import load_dotenv

from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey

from anchorpy import Wallet

from driftpy.slot.slot_subscriber import SlotSubscriber
from driftpy.drift_client import DriftClient
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.auction_subscriber.auction_subscriber import AuctionSubscriber
from driftpy.auction_subscriber.types import AuctionSubscriberConfig
from driftpy.user_map.user_map import UserMap
from driftpy.user_map.user_map_config import UserMapConfig, WebsocketConfig
from driftpy.dlob.dlob_client import DLOBClient
from driftpy.dlob.client_types import DLOBClientConfig
from driftpy.constants.config import DriftEnv
from driftpy.keypair import load_keypair

from jit_proxy.jitter.jitter_shotgun import JitterShotgun  # type: ignore
from jit_proxy.jitter.jitter_sniper import JitterSniper  # type: ignore
from jit_proxy.jitter.base_jitter import JitParams  # type: ignore
from jit_proxy.jit_proxy_client import JitProxyClient, PriceType  # type: ignore

from keepyr_types import Bot, JitMakerConfig
from keepyr_utils import (
    get_best_limit_ask_exclusionary,
    get_best_limit_bid_exclusionary,
)

from jit_maker.src.utils import calculate_base_amount_to_mm, is_market_volatile  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JitMaker(Bot):
    def __init__(
        self,
        config: JitMakerConfig,
        drift_client: DriftClient,
        usermap: UserMap,
        jitter: Union[JitterSniper, JitterShotgun],
        drift_env: DriftEnv,
    ):
        self.drift_env = drift_env

        # Default values for some attributes
        self.lookup_tables = None
        self.tasks: list[asyncio.Task] = []
        self.default_interval_ms = 30_000

        # Set up locks, etc.
        self.task_lock = asyncio.Lock()
        self.watchdog = asyncio.Lock()
        self.watchdog_last_pat = time.time()

        # Set up attributes from config
        self.name = config.bot_id
        self.dry_run = config.dry_run
        self.sub_accounts = config.sub_accounts
        self.perp_market_indexes = config.perp_market_indexes

        # Set up clients & subscriptions
        self.drift_client = drift_client
        self.usermap = usermap
        self.jitter = jitter

        self.slot_subscriber = SlotSubscriber(self.drift_client)

        dlob_config = DLOBClientConfig(
            self.drift_client, self.usermap, self.slot_subscriber, 30
        )

        self.dlob_client = DLOBClient("https://dlob.drift.trade", dlob_config)

    async def init(self):
        logger.info(f"Initializing {self.name}")

        await self.slot_subscriber.subscribe()
        await self.dlob_client.subscribe()

        self.lookup_tables = await self.drift_client.fetch_market_lookup_table()

        logger.info(f"Initialized {self.name}")

    async def reset(self):
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.tasks.clear()
        logger.info(f"{self.name} reset complete")

    async def start_interval_loop(self, interval_ms: int | None = 1000):
        async def interval_loop():
            try:
                while True:
                    await self.run_periodic_tasks()
                    await asyncio.sleep(interval_ms / 1000)
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(interval_loop())
        self.tasks.append(task)
        logger.info(f"{self.name} Bot started! driftEnv: {self.drift_env}")

    async def health_check(self):
        healthy = False
        async with self.watchdog:
            healthy = self.watchdog_last_pat > time.time() - (
                2 * self.default_interval_ms // 1_000
            )
        return healthy

    async def run_periodic_tasks(self):
        start = time.time()
        ran = False
        try:
            async with self.task_lock:
                logger.info(
                    f"{datetime.fromtimestamp(start).isoformat()} running jit periodic tasks"
                )
                # simulate some task
                await asyncio.sleep(1)
                ran = True
        except Exception as e:
            raise e
        finally:
            if ran:
                duration = time.time() - start
                logger.info(f"{self.name} took {duration} s to run")

                async with self.watchdog:
                    self.watchdog_last_pat = time.time()


async def main():
    load_dotenv()
    secret = os.getenv("PRIVATE_KEY")
    url = os.getenv("RPC_URL")

    kp = load_keypair(secret)
    wallet = Wallet(kp)

    connection = AsyncClient(url)

    drift_client = DriftClient(
        connection,
        wallet,
        "mainnet",
        account_subscription=AccountSubscriptionConfig("websocket"),
    )

    usermap_config = UserMapConfig(drift_client, WebsocketConfig())
    usermap = UserMap(usermap_config)

    auction_subscriber = AuctionSubscriber(AuctionSubscriberConfig(drift_client))

    jit_proxy_client = JitProxyClient(
        drift_client,
        Pubkey.from_string("J1TnP8zvVxbtF5KFp5xRmWuvG9McnhzmBd9XGfCyuxFP"),
    )

    jitter = JitterShotgun(drift_client, auction_subscriber, jit_proxy_client)

    jit_params = JitParams(
        bid=-1_000_000,
        ask=1_010_000,
        min_position=0,
        max_position=2,
        price_type=PriceType.Oracle(),
        sub_account_id=None,
    )

    jitter.update_perp_params(0, jit_params)
    jitter.update_spot_params(0, jit_params)

    jit_maker_config = JitMakerConfig("jit maker", False, [0], [0])

    jit_maker = JitMaker(jit_maker_config, drift_client, usermap, jitter, "mainnet")

    await jit_maker.init()

    await jit_maker.start_interval_loop(10_000)
    await asyncio.sleep(30)
    print(await jit_maker.health_check())
    await jit_maker.reset()

    # quick & dirty way to keep event loop open
    # try:
    #     while True:
    #         await asyncio.sleep(3600)
    # except asyncio.CancelledError:
    #     pass

    print("Hello world")


if __name__ == "__main__":
    asyncio.run(main())
