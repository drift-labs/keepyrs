import asyncio
import time
import os
import traceback
from typing import Callable, Union

from dotenv import load_dotenv  # type: ignore
from aiohttp import web

from solana.rpc.types import TxOpts
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Processed

from anchorpy import Wallet

from driftpy.drift_client import DriftClient
from driftpy.drift_user import DriftUser
from driftpy.user_map.user_map import UserMap
from driftpy.user_map.user_map_config import UserMapConfig, WebsocketConfig
from driftpy.types import TxParams
from driftpy.account_subscription_config import AccountSubscriptionConfig
from driftpy.tx.fast_tx_sender import FastTxSender
from driftpy.addresses import get_user_account_public_key
from driftpy.keypair import load_keypair
from driftpy.math.bankruptcy import is_user_bankrupt  # type: ignore

from keepyr_types import LiquidatorConfig
from keepyr_utils import start_server

from liquidator.src.utils import *

from custom_log import get_custom_logger

logger = get_custom_logger(__name__)

FIFTEEN_SECONDS = 15


class Liquidator(LiquidatorConfig):
    def __init__(self, config: LiquidatorConfig):
        self.name = config.bot_id

        self.drift_client = config.drift_client
        self.usermap = config.usermap

        self.perp_market_indexes = config.perp_market_indexes
        self.spot_market_indexes = config.spot_market_indexes

        self.active_sub_account = config.active_sub_account
        self.all_sub_accounts = config.all_sub_accounts

        self.perp_market_to_sub_account = config.perp_market_to_sub_account
        self.spot_market_to_sub_account = config.spot_market_to_sub_account

        self.tasks: list[asyncio.Task] = []
        self.task_lock = asyncio.Lock()
        self.watchdog = asyncio.Lock()
        self.watchdog_last_pat = time.time()

        self.max_pos_takeover_pct_num: int = 100
        self.max_pos_takeover_pct_den: int = 100
        self.min_deposit_amount_to_liq = config.min_deposit_to_liq or {}

    async def init(self):
        logger.info(f"Initializing {self.name}")

        await self.drift_client.subscribe()

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

    def get_sub_account_id_to_liquidate_spot(
        self, market_index: int
    ) -> Union[int, None]:
        return self.spot_market_to_sub_account.get(market_index, None)

    def get_sub_account_id_to_liquidate(self, market_index: int) -> Union[int, None]:
        return self.perp_market_to_sub_account.get(market_index, None)

    async def start_interval_loop(self, interval_ms: int = 6_000):
        self.tasks.append(
            asyncio.create_task(self.spawn(self.try_liquidate, interval_ms / 1_000))
        )
        self.tasks.append(
            asyncio.create_task(
                self.spawn(self.try_resolve_bankruptcies, FIFTEEN_SECONDS)
            )
        )

        logger.info(f"{self.name} Bot started!")

    async def spawn(self, func: Callable, interval: int):
        try:
            while True:
                await func()  # either try_liquidate_start or resolve_bankrupt_users
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            pass

    async def try_liquidate(self):
        async with self.task_lock:
            liquidate_start = time.time()
            liq_users: list[LiquidatableUser] = []
            checked_users = 0
            liquidated_users = 0

            for user in self.usermap.values():
                (
                    can_be_liquidated,
                    margin_requirement,
                    total_collateral,
                ) = user.can_be_liquidated()

                is_bankrupt = is_user_bankrupt(user)

                if (
                    can_be_liquidated or user.is_being_liquidated()
                ) and not is_bankrupt:
                    user_key = str(user.user_public_key)
                    liquidated_users += 1
                    liq_users.append(
                        LiquidatableUser(
                            user,
                            user_key,
                            margin_requirement,
                            can_be_liquidated,
                            total_collateral,
                        )
                    )

            liq_users = sorted(
                liq_users, key=lambda user: user.margin_requirement, reverse=True
            )

            for liq_user in liq_users:
                if liq_user.can_be_liquidated:
                    logger.info(f"liquidating user: {liq_user.user_key}")
                    logger.info(
                        f"total collateral: {liq_user.total_collateral} maintenance requirement: {liq_user.margin_requirement}"
                    )

                    liquidator = self.drift_client.get_user()
                    liquidatee_user_account = liq_user.user.get_user_account()

                    (
                        deposit_market_index_to_liq,
                        deposit_amount_to_liq,
                        index_with_max_assets,
                        index_with_open_orders,
                    ) = find_best_spot_position(
                        self.drift_client,
                        liq_user.user,
                        liquidator,
                        liquidatee_user_account.spot_positions,
                        False,
                        self.max_pos_takeover_pct_num,
                        self.max_pos_takeover_pct_den,
                        self.min_deposit_amount_to_liq,
                    )

                    (
                        borrow_amount_index_to_liq,
                        borrow_amount_to_liq,
                        _,
                        _,
                    ) = find_best_spot_position(
                        self.drift_client,
                        liq_user.user,
                        liquidator,
                        liquidatee_user_account.spot_positions,
                        True,
                        self.max_pos_takeover_pct_num,
                        self.max_pos_takeover_pct_den,
                        self.min_deposit_amount_to_liq,
                    )

                    liquidatee_has_spot_pos = False
                    if (
                        borrow_amount_index_to_liq != -1
                        and deposit_market_index_to_liq != -1
                    ):
                        liquidatee_has_spot_pos = True
                        logger.info(
                            f"user {liq_user.user_key} has spot position to liquidate, margin requirement: {liq_user.margin_requirement}"
                        )

                        await liquidate_borrow(
                            self,
                            deposit_market_index_to_liq,
                            borrow_amount_index_to_liq,
                            borrow_amount_to_liq,
                            liq_user.user,
                        )

                    usdc_market = self.drift_client.get_spot_market_account(0)

                    if not usdc_market:
                        logger.error("usdc market not found")
                        continue

                    for liq_position in liq_user.user.get_active_perp_positions():
                        (
                            liquidatee_has_perp_pos,
                            liquidatee_has_unsettled_perp_pnl,
                            liquidatee_has_lp_pos,
                            liquidatee_perp_has_open_orders,
                            liquidatee_perp_index_with_open_orders,
                        ) = get_position_stats(liq_position)

                        if liquidatee_has_unsettled_perp_pnl:
                            perp_market = self.drift_client.get_perp_market_account(
                                liq_position.market_index
                            )

                            if not is_variant(perp_market.status, "Active"):
                                continue

                            if not perp_market:
                                logger.error(
                                    f"perp market {liq_position.market_index} not found"
                                )
                                continue

                            await liquidate_perp_pnl(
                                self,
                                liq_user.user,
                                perp_market,
                                usdc_market,
                                liq_position,
                                deposit_market_index_to_liq,
                                deposit_amount_to_liq,
                                borrow_amount_index_to_liq,
                                borrow_amount_to_liq,
                            )

                        base_amount_to_liquidate = calculate_base_amount_to_liquidate(
                            self.drift_client,
                            liquidator,
                            liq_user.user.get_perp_position_with_lp_settle(
                                liq_position.market_index, liq_position
                            )[0],
                            self.max_pos_takeover_pct_num,
                            self.max_pos_takeover_pct_den,
                        )

                        sub_account_to_liq_perp = self.get_sub_account_id_to_liquidate(
                            liq_position.market_index
                        )

                        if sub_account_to_liq_perp is None:
                            logger.error(
                                f"sub account not found for perp market: {liq_position.market_index}"
                            )
                            continue

                        if base_amount_to_liquidate > 0:
                            await liquidate_perp(
                                self,
                                liq_user,
                                liq_position,
                                sub_account_to_liq_perp,
                                base_amount_to_liquidate,
                            )

                        elif liquidatee_has_lp_pos:
                            await clear_lp_pos(
                                self, liq_user, liq_position, sub_account_to_liq_perp
                            )

                        if not liquidatee_has_perp_pos and not liquidatee_has_spot_pos:
                            logger.info(
                                f"user {liq_user.user_key} can be liquidated but has no positions"
                            )

                            if liquidatee_perp_has_open_orders:
                                sub_account_to_liq_perp = (
                                    self.get_sub_account_id_to_liquidate(
                                        liquidatee_perp_index_with_open_orders
                                    )
                                )
                                if sub_account_to_liq_perp is None:
                                    logger.error(
                                        f"sub account not found for perp market: {liquidatee_perp_index_with_open_orders}"
                                    )
                                    continue
                                await clear_perp_pos(
                                    self,
                                    liq_user,
                                    liquidatee_perp_index_with_open_orders,
                                    sub_account_to_liq_perp,
                                )

                            if index_with_open_orders != -1:
                                sub_account_to_liq_perp = (
                                    self.get_sub_account_id_to_liquidate_spot(
                                        index_with_max_assets
                                    )
                                )
                                if sub_account_to_liq_perp is None:
                                    logger.error(
                                        f"sub account not found for spot market: {index_with_max_assets}"
                                    )
                                    break
                                await liquidate_spot_with_oo(
                                    self,
                                    liq_user,
                                    index_with_max_assets,
                                    sub_account_to_liq_perp,
                                )

                elif user.is_being_liquidated():
                    await clear_liquidation(
                        self, liq_user, index_with_max_assets, index_with_open_orders
                    )

            logger.info(
                f"bot checked {checked_users} users "
                f"identified {len(liq_users)} liquidatable users "
                f"in {time.time() - liquidate_start:.2f}s"
            )
            self.watchdog_last_pat = time.time()

    async def try_resolve_bankruptcies(self):
        async with self.task_lock:
            bankruptcy_start = time.time()
            bankrupt_users: list[BankruptUser] = []
            bankrupt_users_count = 0

            for user in self.usermap.values():
                is_bankrupt = is_user_bankrupt(user)

                if is_bankrupt:
                    user_key = str(user.user_public_key)
                    bankrupt_users.append(BankruptUser(user, user_key))
                    bankrupt_users_count += 1

            for bankrupt_user in bankrupt_users:
                user_account = bankrupt_user.user.get_user_account()
                user_key = get_user_account_public_key(self.drift_client.program_id, user_account.authority)  # type: ignore
                bankrupt_perp_markets = find_perp_bankrupting_markets(
                    self.drift_client, user
                )
                bankrupt_spot_markets = find_spot_bankrupting_markets(
                    self.drift_client, user
                )

                logger.info(
                    f"user: {user_key} is bankrupt in perp markets: {bankrupt_perp_markets} and spot markets: {bankrupt_spot_markets}"
                )

                for market_index in bankrupt_perp_markets:
                    logger.info(
                        f"resolving perp bankruptcy in market: {market_index} for user: {str(user_key)}"
                    )
                    start = time.time()

                    try:
                        sig = await self.drift_client.resolve_perp_bankruptcy(
                            user_account.authority, market_index
                        )
                        logger.success(
                            f"successfully resolved perp bankruptcy in market: {market_index} for user: {str(user_key)}"
                        )
                        logger.success(sig)
                    except Exception as e:
                        logger.error(
                            f"failed to resolve perp bankruptcy in market: {market_index} for user: {str(user_key)}"
                        )
                        logger.error(e)
                    logger.info(
                        f"finished resolving for perp market: {market_index} in {start - time.time()}s"
                    )

                for market_index in bankrupt_spot_markets:
                    logger.info(
                        f"resolving spot bankruptcy in market: {market_index} for user: {str(user_key)}"
                    )
                    start = time.time()

                    try:
                        sig = await self.drift_client.resolve_spot_bankruptcy(
                            user_account.authority, market_index
                        )
                        logger.success(
                            f"successfully resolved spot bankruptcy in market: {market_index} for user: {str(user_key)}"
                        )
                        logger.success(sig)
                    except Exception as e:
                        logger.error(
                            f"failed to resolve spot bankruptcy in market: {market_index} for user: {str(user_key)}"
                        )
                        logger.error(e)
                    logger.info(
                        f"finished resolving for spot market: {market_index} in {start - time.time()}s"
                    )

            logger.info(
                f"resolved {bankrupt_users_count} bankrupt users in {time.time() - bankruptcy_start:.2f}s"
            )
            self.watchdog_last_pat = time.time()


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
        tx_params=TxParams(700_000, 100_000_000),  # crank priority fees way up
        opts=tx_opts,
        tx_sender=fast_tx_sender,
    )

    usermap_config = UserMapConfig(drift_client, WebsocketConfig())
    usermap = UserMap(usermap_config)

    await usermap.subscribe()

    perps = {
        9: 0
        # 0: 0,
        # 1: 0,
        # 2: 0,
        # 3: 0,
        # 4:0,
        # 5:0,
        # 6:0,
        # 7:0,
        # 8:0,
        # 9:0,
        # 10:0
    }

    spot = {
        # 9:0
        0: 0,
        1: 0,
        2: 0,
        3: 0,
        4: 0,
        5: 0,
        6: 0,
        7: 0,
        8: 0,
        9: 0,
        10: 0,
    }

    liquidator_config = LiquidatorConfig(
        "liquidator",
        drift_client,
        usermap,
        [9],
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        0,
        [0],
        perps,
        spot,
    )

    liquidator = Liquidator(liquidator_config)

    asyncio.create_task(start_server(liquidator))

    await liquidator.init()

    await liquidator.start_interval_loop(10_000)

    await asyncio.gather(*liquidator.get_tasks())

    print(f"Healthy?: {await liquidator.health_check()}")
    await liquidator.reset()

    print("Hello world")


if __name__ == "__main__":
    asyncio.run(main())
