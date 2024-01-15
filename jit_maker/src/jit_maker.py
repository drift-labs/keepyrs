import asyncio
import math
import os
import logging
import time

from datetime import datetime
from typing import Union
from dotenv import load_dotenv
from aiohttp import web

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
from driftpy.dlob.dlob_subscriber import DLOBSubscriber
from driftpy.dlob.client_types import DLOBClientConfig
from driftpy.types import MarketType
from driftpy.constants.config import DriftEnv
from driftpy.constants.numeric_constants import BASE_PRECISION
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

from jit_maker.src.utils import calculate_base_amount_to_mm, is_market_volatile, rebalance  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TARGET_LEVERAGE_PER_ACCOUNT = 1
BASE_PCT_DEVIATION_BEFORE_HEDGE = 0.1


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
        self.spot = config.spot

        # Set up clients & subscriptions
        self.drift_client = drift_client
        self.usermap = usermap
        self.jitter = jitter

        self.slot_subscriber = SlotSubscriber(self.drift_client)

        dlob_config = DLOBClientConfig(
            self.drift_client, self.usermap, self.slot_subscriber, 30
        )

        self.dlob_subscriber = DLOBSubscriber("https://dlob.drift.trade", dlob_config)

    async def init(self):
        logger.info(f"Initializing {self.name}")

        await self.drift_client.subscribe()
        await self.slot_subscriber.subscribe()
        await self.dlob_subscriber.subscribe()
        await self.jitter.subscribe()

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

                for i in range(len(self.perp_market_indexes)):
                    perp_idx = self.perp_market_indexes[i]
                    sub_id = self.sub_accounts[i]
                    self.drift_client.switch_active_user(sub_id)

                    drift_user = self.drift_client.get_user(sub_id)
                    perp_market_account = self.drift_client.get_perp_market_account(
                        perp_idx
                    )
                    oracle_price_data = (
                        self.drift_client.get_oracle_price_data_for_perp_market(
                            perp_idx
                        )
                    )

                    num_markets_for_subaccount = len(
                        [num for num in self.sub_accounts if num == sub_id]
                    )

                    if num_markets_for_subaccount == 0:
                        logger.error(f"0 markets found for sub_account_id: {sub_id}")
                        continue

                    target_leverage = (
                        TARGET_LEVERAGE_PER_ACCOUNT / num_markets_for_subaccount
                    )
                    actual_leverage = drift_user.get_leverage() / 10_000

                    max_base = calculate_base_amount_to_mm(
                        perp_market_account,
                        drift_user.get_net_spot_market_value(None),
                        target_leverage,
                    )

                    overlevered_long = False
                    overlevered_short = False
                    if actual_leverage >= target_leverage:
                        logger.warning(
                            f"jit maker at or above max leverage actual: {actual_leverage} target: {target_leverage}"
                        )
                        overlevered_base_asset_amount = drift_user.get_perp_position(
                            perp_idx
                        ).base_asset_amount
                        if overlevered_base_asset_amount > 0:
                            overlevered_long = True
                        elif overlevered_base_asset_amount < 0:
                            overlevered_short = True

                    # check whether or not maker hedges
                    match perp_idx:
                        case 0:
                            spot_idx = 1
                        case 1:
                            spot_idx = 3
                        case 2:
                            spot_idx = 4
                        case _:
                            spot_idx = 0

                    def user_filter(user_account, user_key: str, order) -> bool:
                        skip = user_key == str(drift_user.user_public_key)

                        if is_market_volatile(
                            perp_market_account, oracle_price_data, 0.015  # 150 bps
                        ):
                            logger.info(
                                f"Skipping, Market: {perp_market_account.market_index} is volatile"
                            )
                            skip = True

                        if skip:
                            logger.info(f"Skipping user: {user_key}")

                        return skip

                    self.jitter.set_user_filter(user_filter)

                    best_bid = get_best_limit_bid_exclusionary(
                        self.dlob_subscriber.dlob,
                        perp_market_account.market_index,
                        MarketType.Perp(),
                        oracle_price_data.slot,
                        oracle_price_data,
                        str(drift_user.user_public_key),
                    )

                    best_ask = get_best_limit_ask_exclusionary(
                        self.dlob_subscriber.dlob,
                        perp_market_account.market_index,
                        MarketType.Perp(),
                        oracle_price_data.slot,
                        oracle_price_data,
                        str(drift_user.user_public_key),
                    )

                    if not best_bid or not best_ask:
                        logger.warning("skipping, no best bid / ask")
                        return

                    best_bid_price = best_bid.get_price(
                        oracle_price_data, self.dlob_subscriber.slot_source.get_slot()
                    )

                    best_ask_price = best_ask.get_price(
                        oracle_price_data, self.dlob_subscriber.slot_source.get_slot()
                    )

                    logger.info(f"best bid price: {best_bid_price}")
                    logger.info(f"best ask price: {best_ask_price}")

                    logger.info(f"oracle price: {oracle_price_data.price}")

                    bid_offset = math.floor(
                        best_bid_price - (0.99 * oracle_price_data.price)
                    )
                    ask_offset = math.floor(
                        best_ask_price - (1.01 * oracle_price_data.price)
                    )

                    logger.info(f"max_base: {max_base}")

                    perp_min_position = math.floor((-max_base) * BASE_PRECISION)
                    perp_max_position = math.floor((max_base) * BASE_PRECISION)
                    if overlevered_long:
                        perp_max_position = 0
                    elif overlevered_short:
                        perp_min_position = 0

                    new_perp_params = JitParams(
                        bid=bid_offset,
                        ask=ask_offset,
                        min_position=perp_min_position,
                        max_position=perp_max_position,
                        price_type=PriceType.Oracle(),
                        sub_account_id=sub_id,
                    )

                    self.jitter.update_perp_params(perp_idx, new_perp_params)
                    logger.info(
                        f"jitter perp params updated, market_index: {perp_idx}, bid: {new_perp_params.bid}, ask: {new_perp_params.ask} "
                        f"min_position: {new_perp_params.min_position}, max_position: {new_perp_params.max_position}"
                    )

                    if spot_idx != 0 and self.spot:
                        spot_market_account = self.drift_client.get_spot_market_account(
                            spot_idx
                        )

                        spot_market_precision = 10**spot_market_account.decimals

                        spot_min_position = math.floor(
                            (-max_base) * spot_market_precision
                        )
                        spot_max_position = math.floor(
                            (max_base) * spot_market_precision
                        )
                        if overlevered_long:
                            spot_max_position = 0
                        elif overlevered_short:
                            spot_min_position = 0

                        new_spot_params = JitParams(
                            bid=min(bid_offset, -1),
                            ask=max(ask_offset, 1),
                            min_position=spot_min_position,
                            max_position=spot_max_position,
                            price_type=PriceType.Oracle(),
                            sub_account_id=sub_id,
                        )

                        self.jitter.update_spot_params(spot_idx, new_spot_params)
                        logger.info(
                            f"jitter spot params updated, market_index: {spot_idx}, bid: {new_spot_params.bid}, ask: {new_spot_params.ask} "
                            f"min_position: {new_spot_params.min_position}, max_position: {new_spot_params.max_position}"
                        )

                        if (
                            self.drift_client.active_sub_account_id
                            == self.sub_accounts[i]
                        ):
                            max_size = 200
                            if spot_idx == 1:
                                max_size *= 2

                            await rebalance(
                                self.drift_client,
                                drift_user,
                                perp_idx,
                                spot_idx,
                                max_size,
                                max_base * BASE_PCT_DEVIATION_BEFORE_HEDGE,
                            )

                await asyncio.sleep(10)

                logger.info(f"done: {time.time() - start}s")
                ran = True
        except Exception as e:
            raise e
        finally:
            if ran:
                duration = time.time() - start
                logger.info(f"{self.name} took {duration} s to run")

                async with self.watchdog:
                    self.watchdog_last_pat = time.time()


def make_health_check_handler(jit_maker):
    async def health_check_handler(request):
        healthy = await jit_maker.health_check()
        if healthy:
            return web.Response(status=200)  # OK status for healthy
        else:
            return web.Response(status=503)  # Service Unavailable for unhealthy

    return health_check_handler


async def start_server(jit_maker):
    app = web.Application()
    health_handler = make_health_check_handler(jit_maker)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()


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

    await usermap.subscribe()

    auction_subscriber = AuctionSubscriber(AuctionSubscriberConfig(drift_client))

    jit_proxy_client = JitProxyClient(
        drift_client,
        Pubkey.from_string("J1TnP8zvVxbtF5KFp5xRmWuvG9McnhzmBd9XGfCyuxFP"),
    )

    jitter = JitterShotgun(drift_client, auction_subscriber, jit_proxy_client, True)

    jit_maker_config = JitMakerConfig("jit maker", False, [0], [0], False)

    for sub_id in jit_maker_config.sub_accounts:
        await drift_client.add_user(sub_id)

    jit_maker = JitMaker(jit_maker_config, drift_client, usermap, jitter, "mainnet")

    asyncio.create_task(start_server(jit_maker))

    await jit_maker.init()

    await jit_maker.start_interval_loop(10_000)

    await asyncio.gather(*jit_maker.get_tasks())

    print(f"Healthy?: {await jit_maker.health_check()}")
    await jit_maker.reset()

    print("Hello world")


if __name__ == "__main__":
    asyncio.run(main())
