import time

from typing import Tuple

from driftpy.drift_client import DriftClient
from driftpy.drift_user import DriftUser
from driftpy.types import *
from driftpy.constants.numeric_constants import *
from driftpy.math.spot_position import get_token_amount
from driftpy.math.margin import MarginCategory
from driftpy.math.perp_position import calculate_claimable_pnl  # type: ignore
from driftpy.math.tiers import get_perp_market_tier_number, perp_tier_is_as_safe_as  # type: ignore

from custom_log import get_custom_logger

logger = get_custom_logger(__name__)


@dataclass
class LiquidatableUser:
    user: DriftUser
    user_key: str
    margin_requirement: int
    can_be_liquidated: bool


def calculate_spot_token_amount_to_liquidate(
    drift_client: DriftClient,
    liquidator_user: DriftUser,
    liquidatee_position: SpotPosition,
    max_pos_takeover_pct_num: int,
    max_pos_takeover_pct_den: int,
) -> int:
    spot_market = drift_client.get_spot_market_account(liquidatee_position.market_index)

    if not spot_market:
        logger.error(
            f"Could not find spot market for index {liquidatee_position.market_index}"
        )

    token_precision = 10**spot_market.decimals  # type: ignore

    oracle_price = drift_client.get_oracle_price_data_for_spot_market(liquidatee_position.market_index).price  # type: ignore

    collateral_to_spend = (
        liquidator_user.get_free_collateral()
        * PRICE_PRECISION
        * max_pos_takeover_pct_num
        * token_precision
    )

    max_spend_token_amount_to_liquidate = collateral_to_spend // (
        oracle_price * QUOTE_PRECISION * max_pos_takeover_pct_den
    )

    liquidatee_token_amount = get_token_amount(liquidatee_position.scaled_balance, spot_market, liquidatee_position.balance_type)  # type: ignore

    return min(max_spend_token_amount_to_liquidate, liquidatee_token_amount)


def calculate_base_amount_to_liquidate(
    drift_client: DriftClient,
    liquidator_user: DriftUser,
    liquidatee_position: PerpPosition,
    max_pos_takeover_pct_num: int,
    max_pos_takeover_pct_den: int,
) -> int:
    oracle_price = drift_client.get_oracle_price_data_for_perp_market(liquidatee_position.market_index).price  # type: ignore

    collateral_to_spend = (
        liquidator_user.get_free_collateral()
        * PRICE_PRECISION
        * max_pos_takeover_pct_num
        * BASE_PRECISION
    )

    base_asset_amount_to_liquidate = collateral_to_spend // (
        oracle_price * QUOTE_PRECISION * max_pos_takeover_pct_den
    )

    return min(
        abs(liquidatee_position.base_asset_amount), base_asset_amount_to_liquidate
    )


def find_best_spot_position(
    drift_client: DriftClient,
    liquidatee_user: DriftUser,
    liquidator_user: DriftUser,
    spot_positions: list[SpotPosition],
    is_borrow: bool,
    position_takeover_pct_num: int,
    position_takeover_pct_den: int,
    min_deposit_to_liq: dict[int, int],
) -> Tuple[int, int, int, int]:
    best_index = -1
    best_amount = 0
    current_asset_weight = 0
    current_liability_weight = 0
    index_with_max_assets = -1
    max_assets = -1
    index_with_open_orders = -1

    for position in spot_positions:
        if position.scaled_balance == 0:
            # We do not care.
            continue

        min_amount = min_deposit_to_liq.get(
            position.market_index, 0
        )  # skip any position less than configured min for market

        logger.info(
            f"find best spot position: min_amount: {min_amount} market: {position.market_index}"
        )

        if abs(position.scaled_balance) < min_amount:
            logger.info(
                f"find best spot position: skipping market: {position.market_index} due to min_amount: {min_amount}"
            )
            continue

        market = drift_client.get_spot_market_account(position.market_index)

        if not market:
            logger.error(
                f"Could not find spot market for index {position.market_index}"
            )
            continue

        if position.open_orders > 0:
            index_with_open_orders = position.market_index

        total_asset_value = liquidatee_user.get_spot_market_asset_value(
            position.market_index, MarginCategory.MAINTENANCE, True
        )

        if abs(total_asset_value) > max_assets:
            max_assets = abs(total_asset_value)
            index_with_max_assets = position.market_index

        if (is_borrow and is_variant(position.balance_type, "Deposit")) or (
            not is_borrow and is_variant(position.balance_type, "Borrow")
        ):
            continue

        token_amount = calculate_spot_token_amount_to_liquidate(
            drift_client,
            liquidator_user,
            position,
            position_takeover_pct_num,
            position_takeover_pct_den,
        )

        if is_borrow:
            if market.maintenance_liability_weight < current_liability_weight:
                best_amount = token_amount
                best_index = position.market_index
                current_liability_weight = market.maintenance_liability_weight
                current_asset_weight = market.maintenance_asset_weight
        else:
            if market.maintenance_asset_weight > current_asset_weight:
                best_amount = token_amount
                best_index = position.market_index
                current_liability_weight = market.maintenance_liability_weight
                current_asset_weight = market.maintenance_asset_weight

    return best_index, best_amount, index_with_max_assets, index_with_open_orders


def find_perp_bankrupting_markets(
    drift_client: DriftClient,
    user: DriftUser,
) -> list[int]:
    bankrupt_indexes: list[int] = []
    for market in drift_client.get_perp_market_accounts():
        position = user.get_perp_position(market.market_index)
        if not position or position.quote_asset_amount >= 0:
            continue
        bankrupt_indexes.append(market.market_index)

    return bankrupt_indexes


def find_spot_bankrupting_markets(
    drift_client: DriftClient, user: DriftUser
) -> list[int]:
    bankrupt_indexes: list[int] = []
    for market in drift_client.get_spot_market_accounts():
        position = user.get_spot_position(market.market_index)
        if position.scaled_balance <= 0 or not is_variant(position.balance_type, "Borrow"):  # type: ignore
            continue
        bankrupt_indexes.append(market.market_index)

    return bankrupt_indexes


async def liquidate_borrow(
    liquidator,
    deposit_market_index_to_liq: int,
    borrow_market_index_to_liq: int,
    borrow_amount_to_liq: int,
    user: DriftUser,
):
    logger.info(
        f"liquidating borrow for user: {user.get_user_account()} value: {borrow_market_index_to_liq}"
    )

    sub_account_id_to_liq_spot = liquidator.get_sub_account_id_to_liquidate_spot(
        borrow_market_index_to_liq
    )

    if not sub_account_id_to_liq_spot:
        logger.info(
            f"no sub account to liquidate for market: {borrow_market_index_to_liq}, skipping"
        )
        return

    start = time.time()
    try:
        sig = await liquidator.drift_client.liquidate_spot(
            user.user_public_key,
            deposit_market_index_to_liq,
            borrow_market_index_to_liq,
            borrow_amount_to_liq,
            liq_sub_account_id=sub_account_id_to_liq_spot,
        )
        logger.success(
            f"successfully liquidated spot for user: {user.user_public_key}, market: {borrow_market_index_to_liq}, amount: {borrow_amount_to_liq}"
        )
        logger.success({sig})
    except Exception as e:
        logger.error(
            f"failed to liquidate spot for user: {user.user_public_key}, market: {borrow_market_index_to_liq}: {e}"
        )
    logger.info(
        f"finished liquidating for spot market: {borrow_market_index_to_liq} in {start - time.time()}s"
    )


async def liquidate_perp_pnl(
    liquidator,
    user: DriftUser,
    perp_market: PerpMarketAccount,
    usdc_market: SpotMarketAccount,
    liquidatee_position: PerpPosition,
    deposit_market_index_to_liq: int,
    deposit_amount_to_liq: int,
    borrow_market_index_to_liq: int,
    borrow_amount_to_liq: int,
):
    logger.info(
        f"liquidating perp pnl user: {str(user.user_public_key)} deposit: {deposit_amount_to_liq} from market: {deposit_market_index_to_liq} borrow: {borrow_amount_to_liq} from market: {borrow_market_index_to_liq}"
    )

    if liquidatee_position.quote_asset_amount > 0:
        claimable_pnl = calculate_claimable_pnl(
            perp_market,
            usdc_market,
            liquidatee_position,
            liquidator.drift_client.get_oracle_price_data_for_perp_market(liquidatee_position.market_index),  # type: ignore
        )

        if claimable_pnl > 0 and borrow_market_index_to_liq == -1:
            start = time.time()
            try:
                sig = await liquidator.drift_client.settle_pnl(
                    user.user_public_key,
                    user.get_user_account(),
                    liquidatee_position.market_index,
                )

                logger.success(
                    f"successfully settled pnl for user: {user.user_public_key}, market: {liquidatee_position.market_index}, amount: {claimable_pnl}"
                )
                logger.success({sig})
            except Exception as e:
                logger.error(
                    f"failed to settle pnl for user: {user.user_public_key}, market: {liquidatee_position.market_index}: {e}"
                )
            logger.info(f"finished settling pnl in {start - time.time()}s")
            return

        frac = 100_000_000
        if claimable_pnl > 0:
            frac = max(liquidatee_position.quote_asset_amount // claimable_pnl, 1)

        if frac < 100_000_000:
            sub_account_to_liq_borrow = liquidator.get_sub_account_id_to_liquidate_spot(
                borrow_market_index_to_liq
            )

            if not sub_account_to_liq_borrow:
                logger.info(
                    f"no sub account to liquidate for market: {borrow_market_index_to_liq}, skipping"
                )
                return

            liquidator.drift_client.switch_active_user(sub_account_to_liq_borrow)

            start = time.time()
            try:
                sig = await liquidator.drift_client.liquidate_borrow_for_perp_pnl(
                    user.user_public_key,
                    user.get_user_account(),
                    liquidatee_position.market_index,
                    borrow_market_index_to_liq,
                    borrow_amount_to_liq // frac,
                )
                logger.success(
                    f"successfully liquidated borrow for perp pnl for user: {user.user_public_key}, market: {liquidatee_position.market_index}, amount: {borrow_amount_to_liq // frac}"
                )
                logger.success({sig})
            except Exception as e:
                logger.error(
                    f"failed to liquidate borrow for perp pnl for user: {user.user_public_key}, market: {liquidatee_position.market_index}: {e}"
                )
            logger.info(f"finished liquidating for perp pnl in {start - time.time()}s")
        else:
            logger.info(
                f"claimable pnl: {claimable_pnl} < liquidatee_position.quote_asset_amount: {liquidatee_position.quote_asset_amount}, skipping"
            )
    else:
        start = time.time()
        safest_perp_tier, safest_spot_tier = user.get_safest_tiers()  # type: ignore

        perp_tier = get_perp_market_tier_number(perp_market)

        if not perp_tier_is_as_safe_as(perp_tier, safest_perp_tier, safest_spot_tier):
            logger.info(
                f"perp tier: {perp_tier} is not as safe as safest perp tier: {safest_perp_tier}, skipping"
            )
            return

        if not deposit_market_index_to_liq in liquidator.spot_market_indexes:
            logger.info(
                f"deposit market: {deposit_market_index_to_liq} not in liquidator.spot_market_indexes: {liquidator.spot_market_indexes}, skipping"
            )
            return

        sub_account_to_takeover_perp_pnl = liquidator.get_sub_account_id_to_liquidate(
            liquidatee_position.market_index
        )

        if not sub_account_to_takeover_perp_pnl:
            logger.info(
                f"no sub account to liquidate for market: {liquidatee_position.market_index}, skipping"
            )
            return

        try:
            sig = await liquidator.drift_client.liquidate_perp_pnl_for_deposit(
                user.user_public_key,
                liquidatee_position.market_index,
                deposit_market_index_to_liq,
                deposit_amount_to_liq,
                liq_sub_account_id=sub_account_to_takeover_perp_pnl,
            )

            logger.success(
                f"successfully liquidated deposit for perp pnl for user: {user.user_public_key}, market: {liquidatee_position.market_index}, amount: {deposit_amount_to_liq}"
            )
            logger.sucess({sig})
        except Exception as e:
            logger.error(
                f"failed to liquidate deposit for perp pnl for user: {user.user_public_key}, market: {liquidatee_position.market_index}: {e}"
            )
