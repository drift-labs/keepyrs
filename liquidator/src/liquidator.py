import asyncio
import time
import os
import traceback
from typing import Union

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

        self.max_pos_takeover_pct_num: int = 50
        self.max_pos_takeover_pct_den: int = 100
        self.min_deposit_amount_to_liq: dict[int, int] = {}

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

    async def start_interval_loop(self, interval_ms: int = 1000):
        async def interval_loop():
            try:
                while True:
                    await self.try_liquidate_start()
                    await asyncio.sleep(interval_ms / 1000)
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(interval_loop())
        self.tasks.append(task)
        logger.info(f"{self.name} Bot started!")

    async def try_liquidate_start(self):
        start = time.time()
        try:
            async with self.task_lock:
                ran, checked_users, liquidatable_users = await self.try_liquidate()
                if ran:
                    logger.info(
                        f"bot checked {checked_users} users "
                        f"identified {liquidatable_users} liquidatable users "
                        f"in {time.time() - start:.2f}s"
                    )
                    self.watchdog_last_pat = time.time()
        except Exception as e:
            logger.error(f"Error in {self.name} try_liquidate: {e}")
            traceback.print_exc()

    async def try_liquidate(self):
        liquidatable_users: list[LiquidatableUser] = []  # type: ignore
        checked_users = 0
        liquidated_users = 0

        for user in self.usermap.values():
            can_be_liquidated, margin_requirement, _ = user.can_be_liquidated()
            if can_be_liquidated or user.is_being_liquidated():
                user_key = str(user.user_public_key)
                liquidated_users += 1
                liquidatable_users.append(
                    LiquidatableUser(
                        user, user_key, margin_requirement, can_be_liquidated
                    )
                )

        liquidatable_users = sorted(
            liquidatable_users, key=lambda user: user.margin_requirement, reverse=True
        )

        for liquidatable_user in liquidatable_users:
            user_account = liquidatable_user.user.get_user_account()
            auth = str(user_account.authority)

            if is_user_bankrupt(user) or user.is_bankrupt():
                await self.try_resolve_bankruptcy(user)
            elif liquidatable_user.can_be_liquidated:
                logger.info(f"liquidating user: {liquidatable_user.user_key}")

                liquidator_user = self.drift_client.get_user()
                liquidatee_user_account = liquidatable_user.user.get_user_account()

                (
                    deposit_market_index_to_liq,
                    deposit_amount_to_liq,
                    index_with_max_assets,
                    index_with_open_orders,
                ) = find_best_spot_position(
                    self.drift_client,
                    liquidatable_user.user,
                    liquidator_user,
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
                    liquidatable_user.user,
                    liquidator_user,
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
                        f"user {liquidatable_user.user_key} has spot position to liquidate, margin requirement: {liquidatable_user.margin_requirement}"
                    )

                    await liquidate_borrow(
                        self,
                        deposit_market_index_to_liq,
                        borrow_amount_index_to_liq,
                        borrow_amount_to_liq,
                        liquidatable_user.user,
                    )

                usdc_market = self.drift_client.get_spot_market_account(0)

                if not usdc_market:
                    logger.error("usdc market not found")
                    continue

                liquidatee_has_perp_pos = False
                liquidatee_has_unsettled_perp_pnl = False
                liquidatee_has_lp_pos = False
                liquidatee_perp_has_open_orders = False
                liquidatee_perp_index_with_open_orders = -1

                for (
                    liquidatee_position
                ) in liquidatable_user.user.get_active_perp_positions():
                    if liquidatee_position.open_orders > 0:
                        liquidatee_perp_has_open_orders = True
                        liquidatee_perp_index_with_open_orders = (
                            liquidatee_position.market_index
                        )

                    liquidatee_has_unsettled_perp_pnl = (
                        liquidatee_position.base_asset_amount == 0
                        and liquidatee_position.quote_asset_amount != 0
                    )
                    liquidatee_has_perp_pos = (
                        liquidatee_position.base_asset_amount != 0
                        and liquidatee_position.quote_asset_amount != 0
                    )
                    liquidatee_has_lp_pos = liquidatee_position.lp_shares != 0

                    if liquidatee_has_unsettled_perp_pnl:
                        perp_market = self.drift_client.get_perp_market_account(
                            liquidatee_position.market_index
                        )

                        if not perp_market:
                            logger.error(
                                f"perp market {liquidatee_position.market_index} not found"
                            )
                            continue

                        await liquidate_perp_pnl(
                            self,
                            liquidatable_user.user,
                            perp_market,
                            usdc_market,
                            liquidatee_position,
                            deposit_market_index_to_liq,
                            deposit_amount_to_liq,
                            borrow_amount_index_to_liq,
                            borrow_amount_to_liq,
                        )

                    base_amount_to_liquidate = calculate_base_amount_to_liquidate(
                        self.drift_client,
                        liquidator_user,
                        liquidatable_user.user.get_perp_position_with_lp_settle(
                            liquidatee_position.market_index, liquidatee_position
                        )[0],
                        self.max_pos_takeover_pct_num,
                        self.max_pos_takeover_pct_den,
                    )

                    sub_account_to_liq_perp = self.get_sub_account_id_to_liquidate(
                        liquidatee_position.market_index
                    )

                    if not sub_account_to_liq_perp:
                        logger.error(
                            f"sub account not found for perp market: {liquidatee_position.market_index}"
                        )
                        continue

                    if base_amount_to_liquidate > 0:
                        start = time.time()
                        try:
                            sig = await self.drift_client.liquidate_perp(
                                liquidatable_user.user.get_user_account().authority,
                                liquidatee_position.market_index,
                                base_amount_to_liquidate,
                                None,
                                liq_sub_account_id=sub_account_to_liq_perp,
                            )
                            logger.success(
                                f"successfully liquidated perp position for user: {liquidatable_user.user_key}"
                            )
                            logger.success({sig})
                        except Exception as e:
                            logger.error(
                                f"failed to liquidate perp position for user: {liquidatable_user.user_key}"
                            )
                            logger.error(e)
                        logger.info(
                            f"finished liquidating perp position in {time.time() - start}s"
                        )

                    elif liquidatee_has_lp_pos:
                        logger.info(
                            f"clearing lp position for user: {liquidatable_user.user_key}"
                        )
                        start = time.time()
                        try:
                            sig = await self.drift_client.liquidate_perp(
                                liquidatable_user.user.get_user_account().authority,
                                liquidatee_position.market_index,
                                0,
                                None,
                                liq_sub_account_id=sub_account_to_liq_perp,
                            )
                        except Exception as e:
                            logger.error(
                                f"failed to clear lp position for user: {liquidatable_user.user_key}"
                            )
                            logger.error(e)
                        logger.info(
                            f"finished clearing lp position in {time.time() - start}s"
                        )

                    if not liquidatee_has_perp_pos and not liquidatee_has_spot_pos:
                        logger.info(
                            f"user {liquidatable_user.user_key} can be liquidated but has no positions"
                        )

                        if liquidatee_perp_has_open_orders:
                            logger.info(
                                f"user {liquidatable_user.user_key} has open orders in perp market: {liquidatee_perp_index_with_open_orders}"
                            )

                            sub_account_to_liq_perp = (
                                self.get_sub_account_id_to_liquidate(
                                    liquidatee_perp_index_with_open_orders
                                )
                            )

                            if not sub_account_to_liq_perp:
                                logger.error(
                                    f"sub account not found for perp market: {liquidatee_perp_index_with_open_orders}"
                                )
                                continue

                            start = time.time()
                            try:
                                sig = await self.drift_client.liquidate_perp(
                                    liquidatable_user.user.get_user_account().authority,
                                    liquidatee_perp_index_with_open_orders,
                                    0,
                                    None,
                                    liq_sub_account_id=sub_account_to_liq_perp,
                                )
                                logger.success(
                                    f"successfully cleared open orders for user: {liquidatable_user.user_key}"
                                )
                                logger.success({sig})
                            except Exception as e:
                                logger.error(
                                    f"failed to clear open orders for user: {liquidatable_user.user_key}"
                                )
                                logger.error(e)
                            logger.info(
                                f"finished clearing open orders in {time.time() - start}s"
                            )

                        if index_with_open_orders == -1:
                            logger.info(
                                f"user {liquidatable_user.user_key} liquidate spot with assets in {index_with_max_assets} and oo in {index_with_open_orders}"
                            )

                            sub_account_to_liq_perp = (
                                self.get_sub_account_id_to_liquidate_spot(
                                    index_with_max_assets
                                )
                            )

                            if not sub_account_to_liq_perp:
                                logger.error(
                                    f"sub account not found for spot market: {index_with_max_assets}"
                                )
                                break

                            start = time.time()
                            try:
                                sig = await self.drift_client.liquidate_spot(
                                    liquidatable_user.user.get_user_account().authority,
                                    index_with_max_assets,
                                    index_with_open_orders,
                                    0,
                                    None,
                                    liq_sub_account_id=sub_account_to_liq_perp,
                                )
                                logger.success(
                                    f"successfully liquidated spot position for user: {liquidatable_user.user_key}"
                                )
                                logger.success({sig})
                            except Exception as e:
                                logger.error(
                                    f"failed to liquidate spot position for user: {liquidatable_user.user_key}"
                                )
                                logger.error(e)
                            logger.info(
                                f"finished liquidating spot position in {time.time() - start}s"
                            )
            elif user.is_being_liquidated():
                logger.info(
                    f"user: {liquidatable_user.user_key} is stuck in liquidation, need to clear it"
                )

                start = time.time()
                try:
                    sig = await self.drift_client.liquidate_spot(
                        liquidatable_user.user.get_user_account().authority,
                        index_with_max_assets,
                        index_with_open_orders,
                        0,
                        None,
                    )
                    logger.success(
                        f"successfully cleared stuck liquidation for user: {liquidatable_user.user.user_key}"
                    )
                    logger.success({sig})
                except Exception as e:
                    logger.error(
                        f"failed to clear stuck liquidation for user: {liquidatable_user.user_key}"
                    )
                    logger.error(e)
                logger.info(
                    f"finished clearing stuck liquidation in {time.time() - start}s"
                )

        return True, checked_users, liquidated_users

    async def try_resolve_bankruptcy(self, user: DriftUser):
        user_account = user.get_user_account()
        user_key = get_user_account_public_key(self.drift_client.program_id, user_account.authority)  # type: ignore

        bankrupt_perp_markets = find_perp_bankrupting_markets(self.drift_client, user)
        bankrupt_spot_markets = find_spot_bankrupting_markets(self.drift_client, user)

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
                logger.success({sig})
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
                logger.success({sig})
            except Exception as e:
                logger.error(
                    f"failed to resolve spot bankruptcy in market: {market_index} for user: {str(user_key)}"
                )
            logger.info(
                f"finished resolving for spot market: {market_index} in {start - time.time()}s"
            )


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

    liquidator_config = LiquidatorConfig(
        "liquidator",
        drift_client,
        usermap,
        [0],
        [0],
        0,
        [0],
        {0: 0},
        {0: 0},
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
