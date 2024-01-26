import asyncio
import logging
import time
import os
import traceback

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
from perp_filler.src.fill_utils import *
from perp_filler.src.node_utils import *
from perp_filler.src.utils import *

from custom_log import get_custom_logger

logger = get_custom_logger(__name__)


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

        self.filling_nodes: dict[str, int] = {}
        self.triggering_nodes: dict[str, int] = {}
        self.throttled_nodes: dict[str, int] = {}

        self.task_lock = asyncio.Lock()
        self.watchdog = asyncio.Lock()
        self.watchdog_last_pat = time.time()

        self.name = config.bot_id
        self.revert_on_failure = config.revert_on_failure
        self.simulate_tx_for_cu_estimate = config.simulate_tx_for_cu_estimate
        self.polling_interval = config.filler_polling_interval or DEFAULT_INTERVAL_S
        self.use_burst_cu_limit = config.use_burst_cu_limit

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

        self.fill_tx_id = 0
        self.fill_tx_since_burst_cu = 0
        self.last_settle_pnl = 0

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
        self.tasks.append(asyncio.create_task(self.spawn(self.log_throttled, 30)))
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
        start = time.time()
        ran = False
        try:
            async with self.task_lock:
                dlob = get_dlob(self)

                prune_throttled_nodes(self)

                fillable_nodes = []
                triggerable_nodes = []
                for market in self.drift_client.get_perp_market_accounts():
                    # filter out all markets that aren't actively trading
                    if not is_variant(market.status, "Active"):
                        continue
                    try:
                        nodes_to_fill, nodes_to_trigger = get_perp_nodes_for_market(
                            self, market, dlob
                        )

                        fillable_nodes += nodes_to_fill
                        triggerable_nodes += nodes_to_trigger

                    except Exception as e:
                        logger.error(
                            f"{self.name} Failed to get nodes for market: {market.market_index}"
                        )
                        traceback.print_exc()
                        continue

                (
                    filtered_fillable_nodes,
                    filtered_triggerable_nodes,
                ) = filter_perp_nodes_for_market(
                    self, fillable_nodes, triggerable_nodes
                )

                await asyncio.gather(
                    execute_fillable_perp_nodes_for_market(
                        self, filtered_fillable_nodes
                    ),
                    execute_triggerable_perp_nodes_for_market(
                        self, filtered_triggerable_nodes
                    ),
                )

                logger.info(f"done: {time.time() - start}s")
                ran = True
        except Exception as e:
            logger.error(f"{e}")
            raise e
        finally:
            if ran:
                duration = time.time() - start
                logger.info(f"try_fill done, took {duration}s")

                async with self.watchdog:
                    self.watchdog_last_pat = time.time()

    async def settle_pnls(self):
        logger.info("Settle PnLs started")
        user = self.drift_client.get_user()
        market_indexes = [pos.market_index for pos in user.get_active_perp_positions()]
        now = time.time()
        if len(market_indexes) == MAX_POSITIONS_PER_USER:
            if now < self.last_settle_pnl + (SETTLE_POSITIVE_PNL_COOLDOWN_MS // 1_000):
                logger.info("Tried to settle positive PnL, but still cooling down...")
            else:
                settle_tasks = []
                for i in range(0, len(market_indexes), SETTLE_PNL_CHUNKS):
                    chunk = market_indexes[i : i + SETTLE_PNL_CHUNKS]
                try:
                    ixs = [set_compute_unit_limit(MAX_CU_PER_TX)]

                    settle_ixs = await self.drift_client.get_settle_pnl_ixs(
                        user.user_public_key,
                        self.drift_client.get_user_account(),
                        chunk,
                    )

                    ixs.append(settle_ixs)

                    sim_result = await simulate_and_get_tx_with_cus(
                        ixs,
                        self.drift_client,
                        self.drift_client.tx_sender,
                        self.lookup_tables,
                        [],
                        SIM_CU_ESTIMATE_MULTIPLIER,
                        True,
                        self.simulate_tx_for_cu_estimate,
                    )
                    logger.info(f"settle_pnls estimate CUs: {sim_result.cu_estimate}")
                    if self.simulate_tx_for_cu_estimate and sim_result.sim_error:
                        logger.error(f"settle_pnls sim error: {sim_result.sim_error}")
                    else:
                        settle_tasks.append(
                            asyncio.create_task(
                                self.drift_client.tx_sender.send(sim_result.tx)
                            )
                        )
                except Exception as e:
                    logger.error(
                        f"error occurred settling pnl for markets {chunk}: {e}"
                    )
                    pass

                try:
                    results = await asyncio.gather(*settle_tasks)
                    for result in results:
                        logger.info(f"settled pnl, tx sig: {result.tx_sig}")
                except Exception as e:
                    logger.error(f"error settling positive pnls: {e}")
                    # TODO
                    pass
                self.last_settle_pnl = now
        else:
            logger.warning(
                f"active positions less than max, actual: {len(market_indexes)}, max: {MAX_POSITIONS_PER_USER}"
            )

    async def log_throttled(self):
        logger.info("THROTTLED STATS:")
        logger.info(f"{len(self.throttled_nodes)} nodes throttled")
        for key, val in self.throttled_nodes.items():
            logger.info(f"throttled node: {key} -> {val}")


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
        tx_params=TxParams(1_400_000, 20_000),  # crank priority fees way up
    )

    usermap_config = UserMapConfig(drift_client, WebsocketConfig())
    usermap = UserMap(usermap_config)

    await usermap.subscribe()

    perp_filler_config = PerpFillerConfig(
        "perp filler",
        simulate_tx_for_cu_estimate=False,
        use_burst_cu_limit=False,
        revert_on_failure=True,
    )

    perp_filler = PerpFiller(perp_filler_config, drift_client, usermap)

    await perp_filler.init()

    await perp_filler.start_interval_loop()

    await asyncio.gather(*perp_filler.get_tasks())

    print(f"Healthy?: {await perp_filler.health_check()}")
    await perp_filler.reset()

    print("Hello world")


if __name__ == "__main__":
    asyncio.run(main())
