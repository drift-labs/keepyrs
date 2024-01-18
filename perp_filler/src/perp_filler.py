import asyncio
import logging
import time
import os

from typing import Callable, Optional
from dotenv import load_dotenv

from solana.rpc.async_api import AsyncClient

from anchorpy import Wallet

from driftpy.drift_client import DriftClient
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.slot.slot_subscriber import SlotSubscriber
from driftpy.accounts.bulk_account_loader import BulkAccountLoader
from driftpy.user_map.user_map_config import UserMapConfig, WebsocketConfig
from driftpy.user_map.user_map import UserMap
from driftpy.events.event_subscriber import EventSubscriber
from driftpy.dlob.dlob_subscriber import DLOBSubscriber
from driftpy.dlob.client_types import DLOBClientConfig
from driftpy.types import TxParams
from driftpy.priority_fees.priority_fee_subscriber import (
    PriorityFeeSubscriber,
    PriorityFeeConfig,
)
from driftpy.keypair import load_keypair

from keepyr_types import PerpFillerConfig

from perp_filler.src.constants import *


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PerpFiller(PerpFillerConfig):
    def __init__(
        self,
        config: PerpFillerConfig,
        drift_client: DriftClient,
        user_map: UserMap,
        bulk_account_loader: Optional[BulkAccountLoader] = None,
        event_subscriber: Optional[EventSubscriber] = None,
    ):
        self.lookup_tables = None
        self.tasks: list[asyncio.Task] = []

        self.task_lock = asyncio.Lock()
        self.watchdog = asyncio.Lock()
        self.watchdog_last_pat = time.time()

        self.name = config.bot_id
        self.revert_on_failure = config.revert_on_failure
        self.simulate_tx_for_cu_estimate = config.simulate_tx_for_cu_estimate
        self.polling_interval = config.filler_polling_interval or DEFAULT_INTERVAL_S

        self.drift_client = drift_client
        self.slot_subscriber = SlotSubscriber(self.drift_client)
        self.event_subscriber = event_subscriber
        self.bulk_account_loader = bulk_account_loader
        self.user_map = user_map

        dlob_config = DLOBClientConfig(
            self.drift_client, self.user_map, self.slot_subscriber, 30
        )

        self.dlob_subscriber = DLOBSubscriber(config=dlob_config)

        pf_config = PriorityFeeConfig(
            connection=self.drift_client.connection,
            frequency_secs=5,
            addresses=[
                "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6"
            ],  # openbook sol/usdc
        )

        self.priority_fee_subscriber = PriorityFeeSubscriber(pf_config)

    async def init(self):
        logger.info(f"Initializing {self.name}")

        await self.drift_client.subscribe()
        await self.slot_subscriber.subscribe()
        await self.dlob_subscriber.subscribe()
        await self.priority_fee_subscriber.subscribe()
        if self.event_subscriber:
            await self.event_subscriber.subscribe()

        self.lookup_tables = [await self.drift_client.fetch_market_lookup_table()]

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

    def get_tasks(self):
        return self.tasks

    async def health_check(self):
        healthy = False
        async with self.watchdog:
            healthy = self.watchdog_last_pat > time.time() - (
                2 * self.polling_interval // 1_000
            )
        return healthy

    async def start_interval_loop(self):
        self.tasks.append(
            asyncio.create_task(self.spawn(self.try_fill, self.polling_interval))
        )
        self.tasks.append(
            asyncio.create_task(
                self.spawn(self.settle_pnls, SETTLE_POSITIVE_PNL_COOLDOWN_MS // 1_000)
            )
        )
        logger.info(f"{self.name} Bot started!")

    async def spawn(self, func: Callable, interval: int):
        try:
            while True:
                await func()  # either try_fill or settle_pnls
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

    async def try_fill(self):
        logger.info("Try Fill started")

    async def settle_pnls(self):
        logger.info("Settle PnLs started")


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
        tx_params=TxParams(600_000, 5_000),  # crank priority fees way up
    )

    usermap_config = UserMapConfig(drift_client, WebsocketConfig())
    usermap = UserMap(usermap_config)

    await usermap.subscribe()

    perp_filler_config = PerpFillerConfig("perp filler")

    perp_filler = PerpFiller(perp_filler_config, drift_client, usermap)

    await perp_filler.init()

    await perp_filler.start_interval_loop()

    await asyncio.gather(*perp_filler.get_tasks())

    print(f"Healthy?: {await perp_filler.health_check()}")
    await perp_filler.reset()

    print("Hello world")


if __name__ == "__main__":
    asyncio.run(main())
