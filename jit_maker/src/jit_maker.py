import asyncio
import math
import os
import logging
import time

from datetime import datetime
from typing import Union
from dotenv import load_dotenv
from aiohttp import web
from driftpy.math.amm import calculate_bid_ask_price

from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed
from solana.rpc.types import TxOpts
from solders.pubkey import Pubkey

from anchorpy import Wallet

from driftpy.slot.slot_subscriber import SlotSubscriber
from driftpy.drift_client import DriftClient, DEFAULT_TX_OPTIONS
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.auction_subscriber.auction_subscriber import AuctionSubscriber
from driftpy.auction_subscriber.types import AuctionSubscriberConfig
from driftpy.user_map.user_map import UserMap
from driftpy.user_map.user_map_config import UserMapConfig, WebsocketConfig
from driftpy.dlob.dlob_subscriber import DLOBSubscriber
from driftpy.dlob.client_types import DLOBClientConfig
from driftpy.types import is_variant, MarketType, TxParams
from driftpy.constants.config import DriftEnv
from driftpy.constants.numeric_constants import BASE_PRECISION
from driftpy.keypair import load_keypair
from driftpy.tx.fast_tx_sender import FastTxSender  # type: ignore

from jit_proxy.jitter.jitter_shotgun import JitterShotgun  # type: ignore
from jit_proxy.jitter.jitter_sniper import JitterSniper  # type: ignore
from jit_proxy.jitter.base_jitter import JitParams  # type: ignore
from jit_proxy.jit_proxy_client import JitProxyClient, PriceType  # type: ignore

from keepyr_types import Bot, JitMakerConfig
from keepyr_utils import (
    get_best_limit_ask_exclusionary,
    get_best_limit_bid_exclusionary,
)

from jit_maker.src.utils import calculate_base_amount_to_mm_perp, calculate_base_amount_to_mm_spot, is_perp_market_volatile, is_spot_market_volatile  # type: ignore

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
        self.sub_accounts: list[int] = config.sub_accounts  # type: ignore
        self.market_indexes: list[int] = config.market_indexes  # type: ignore
        self.market_type = config.market_type
        self.target_leverage = config.target_leverage
        self.spread = config.spread

        # Set up clients & subscriptions
        self.drift_client = drift_client
        self.usermap = usermap
        self.jitter = jitter

        self.slot_subscriber = SlotSubscriber(self.drift_client)

        dlob_config = DLOBClientConfig(
            self.drift_client, self.usermap, self.slot_subscriber, 30
        )

        self.dlob_subscriber = DLOBSubscriber(config=dlob_config)

    async def init(self):
        logger.info(f"Initializing {self.name}")

        # check for duplicate sub_account_ids
        init_len = len(self.sub_accounts)
        dedup_len = len(list(set(self.sub_accounts)))
        if init_len != dedup_len:
            raise ValueError(
                "You CANNOT make multiple markets with the same sub account id"
            )

        # check to make sure 1:1 unique sub account id to market index ratio
        market_len = len(self.market_indexes)
        if dedup_len != market_len:
            raise ValueError("You must have 1 sub account id per market to jit")

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

                for i in range(len(self.market_indexes)):
                    if is_variant(self.market_type, "Perp"):
                        await self.jit_perp(i)
                    else:
                        await self.jit_spot(i)

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

    async def jit_perp(self, index: int):
        perp_idx = self.market_indexes[index]
        sub_id = self.sub_accounts[index]
        self.drift_client.switch_active_user(sub_id)

        drift_user = self.drift_client.get_user(sub_id)
        perp_market_account = self.drift_client.get_perp_market_account(perp_idx)
        oracle_price_data = self.drift_client.get_oracle_price_data_for_perp_market(
            perp_idx
        )

        num_markets_for_subaccount = len(
            [num for num in self.sub_accounts if num == sub_id]
        )

        target_leverage = self.target_leverage / num_markets_for_subaccount
        actual_leverage = drift_user.get_leverage() / 10_000

        max_base = calculate_base_amount_to_mm_perp(
            perp_market_account,  # type: ignore
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
            ).base_asset_amount  # type: ignore
            if overlevered_base_asset_amount > 0:
                overlevered_long = True
            elif overlevered_base_asset_amount < 0:
                overlevered_short = True

        def user_filter(user_account, user_key: str, order) -> bool:
            skip = user_key == str(drift_user.user_public_key)

            if is_perp_market_volatile(
                perp_market_account, oracle_price_data, 0.015  # type: ignore
            ):
                logger.info(
                    f"Skipping, Market: {perp_market_account.market_index} is volatile"  # type: ignore
                )
                skip = True

            if skip:
                logger.info(f"Skipping user: {user_key}")

            return skip

        self.jitter.set_user_filter(user_filter)

        best_bid = get_best_limit_bid_exclusionary(
            self.dlob_subscriber.dlob,  # type: ignore
            perp_market_account.market_index,  # type: ignore
            MarketType.Perp(),
            oracle_price_data.slot,  # type: ignore
            oracle_price_data,  # type: ignore
            str(drift_user.user_public_key),
            uncross=False,
        )

        best_ask = get_best_limit_ask_exclusionary(
            self.dlob_subscriber.dlob,  # type: ignore
            perp_market_account.market_index,  # type: ignore
            MarketType.Perp(),
            oracle_price_data.slot,  # type: ignore
            oracle_price_data,  # type: ignore
            str(drift_user.user_public_key),
            uncross=False,
        )

        (amm_bid, amm_ask) = calculate_bid_ask_price(perp_market_account.amm, oracle_price_data, True)

        if best_bid is not None:
            best_dlob_price = best_bid.get_price(
                oracle_price_data, self.dlob_subscriber.slot_source.get_slot()  # type: ignore
            )

            if best_dlob_price > amm_ask:
                best_bid_price = amm_ask
            else:
                best_bid_price = max(amm_bid, best_dlob_price)

        else:
            best_bid_price = amm_bid

        if best_ask is not None:
            best_dlob_price = best_ask.get_price(
                oracle_price_data, self.dlob_subscriber.slot_source.get_slot()  # type: ignore
            )

            if best_dlob_price < amm_bid:
                best_ask_price = amm_bid
            else:
                best_ask_price = min(amm_ask, best_dlob_price)
        else:
            best_ask_price = amm_ask

        logger.info(f"best bid price: {best_bid_price}")
        logger.info(f"best ask price: {best_ask_price}")

        logger.info(f"oracle price: {oracle_price_data.price}")  # type: ignore

        bid_offset = math.floor(
            best_bid_price - ((1 + self.spread) * oracle_price_data.price)  # type: ignore
        )
        ask_offset = math.floor(
            best_ask_price - ((1 - self.spread) * oracle_price_data.price)  # type: ignore
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

    async def jit_spot(self, index: int):
        spot_idx = self.market_indexes[index]
        sub_id = self.sub_accounts[index]
        self.drift_client.switch_active_user(sub_id)

        drift_user = self.drift_client.get_user(sub_id)
        spot_market_account = self.drift_client.get_spot_market_account(spot_idx)
        oracle_price_data = self.drift_client.get_oracle_price_data_for_spot_market(
            spot_idx
        )

        num_markets_for_subaccount = len(
            [num for num in self.sub_accounts if num == sub_id]
        )

        target_leverage = self.target_leverage / num_markets_for_subaccount
        actual_leverage = drift_user.get_leverage() / 10_000

        max_base = calculate_base_amount_to_mm_spot(
            spot_market_account,  # type: ignore
            drift_user.get_net_spot_market_value(None),
            target_leverage,
        )

        overlevered_long = False
        overlevered_short = False

        if actual_leverage >= target_leverage:
            logger.warning(
                f"jit maker at or above max leverage actual: {actual_leverage} target: {target_leverage}"
            )
            overlevered_base_asset_amount = drift_user.get_spot_position(
                spot_idx
            ).scaled_balance  # type: ignore
            if overlevered_base_asset_amount > 0:
                overlevered_long = True
            elif overlevered_base_asset_amount < 0:
                overlevered_short = True

        def user_filter(user_account, user_key: str, order) -> bool:
            skip = user_key == str(drift_user.user_public_key)

            if is_spot_market_volatile(
                spot_market_account, oracle_price_data, 0.015  # type: ignore
            ):
                logger.info(
                    f"Skipping, Market: {spot_market_account.market_index} is volatile"  # type: ignore
                )
                skip = True

            if skip:
                logger.info(f"Skipping user: {user_key}")

            return skip

        self.jitter.set_user_filter(user_filter)

        best_bid = get_best_limit_bid_exclusionary(
            self.dlob_subscriber.dlob,  # type: ignore
            spot_market_account.market_index,  # type: ignore
            MarketType.Spot(),
            oracle_price_data.slot,  # type: ignore
            oracle_price_data,  # type: ignore
            str(drift_user.user_public_key),
            uncross=True,
        )

        best_ask = get_best_limit_ask_exclusionary(
            self.dlob_subscriber.dlob,  # type: ignore
            spot_market_account.market_index,  # type: ignore
            MarketType.Spot(),
            oracle_price_data.slot,  # type: ignore
            oracle_price_data,  # type: ignore
            str(drift_user.user_public_key),
            uncross=True,
        )

        if not best_bid or not best_ask:
            logger.warning("skipping, no best bid / ask")
            return

        best_bid_price = best_bid.get_price(
            oracle_price_data, self.dlob_subscriber.slot_source.get_slot()  # type: ignore
        )

        best_ask_price = best_ask.get_price(
            oracle_price_data, self.dlob_subscriber.slot_source.get_slot()  # type: ignore
        )

        logger.info(f"best bid price: {best_bid_price}")
        logger.info(f"best ask price: {best_ask_price}")

        logger.info(f"oracle price: {oracle_price_data.price}")  # type: ignore

        bid_offset = math.floor(
            best_bid_price - ((1 + self.spread) * oracle_price_data.price)  # type: ignore
        )
        ask_offset = math.floor(
            best_ask_price - ((1 - self.spread) * oracle_price_data.price)  # type: ignore
        )

        logger.info(f"max_base: {max_base}")

        spot_market_precision = 10**spot_market_account.decimals  # type: ignore

        spot_min_position = math.floor((-max_base) * spot_market_precision)
        spot_max_position = math.floor((max_base) * spot_market_precision)
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

    commitment = Processed
    tx_opts = TxOpts(skip_confirmation=False, preflight_commitment=commitment)
    fast_tx_sender = FastTxSender(connection, tx_opts, 3)

    drift_client = DriftClient(
        connection,
        wallet,
        "mainnet",
        account_subscription=AccountSubscriptionConfig(
            "websocket", commitment=commitment
        ),
        tx_params=TxParams(700_000, 50_000),  # crank priority fees way up
        opts=tx_opts,
        tx_sender=fast_tx_sender,
    )

    usermap_config = UserMapConfig(drift_client, WebsocketConfig())
    usermap = UserMap(usermap_config)

    await usermap.subscribe()

    auction_subscriber = AuctionSubscriber(
        AuctionSubscriberConfig(drift_client, commitment)
    )

    jit_proxy_client = JitProxyClient(
        drift_client,
        Pubkey.from_string("J1TnP8zvVxbtF5KFp5xRmWuvG9McnhzmBd9XGfCyuxFP"),
    )

    jitter = JitterShotgun(drift_client, auction_subscriber, jit_proxy_client, True)

    # This is an example of a perp JIT maker that will JIT the SOL-PERP market
    jit_maker_perp_config = JitMakerConfig("jit maker", [0], [0], MarketType.Perp(), spread=-.001)

    for sub_id in jit_maker_perp_config.sub_accounts:
        await drift_client.add_user(sub_id)

    jit_maker = JitMaker(
        jit_maker_perp_config, drift_client, usermap, jitter, "mainnet"
    )

    # This is an example of a spot JIT maker that will JIT the SOL market
    # jit_maker_spot_config = JitMakerConfig(
    #     "jit maker", [1], [0], MarketType.Spot()
    # )

    # for sub_id in jit_maker_spot_config.sub_accounts:
    #     await drift_client.add_user(sub_id)

    # jit_maker = JitMaker(
    #     jit_maker_spot_config, drift_client, usermap, jitter, "mainnet"
    # )

    asyncio.create_task(start_server(jit_maker))

    await jit_maker.init()

    await jit_maker.start_interval_loop(10_000)

    await asyncio.gather(*jit_maker.get_tasks())

    print(f"Healthy?: {await jit_maker.health_check()}")
    await jit_maker.reset()

    print("Hello world")


if __name__ == "__main__":
    asyncio.run(main())