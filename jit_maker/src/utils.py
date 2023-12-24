import time
import logging

from driftpy.drift_client import DriftClient
from driftpy.types import (
    PerpMarketAccount,
    OraclePriceData,
    MarketType,
    OrderParams,
    OrderType,
    PositionDirection,
    PostOnlyParams,
)
from driftpy.constants.numeric_constants import (
    PRICE_PRECISION,
    QUOTE_PRECISION,
    PERCENTAGE_PRECISION,
)
from driftpy.math.conversion import convert_to_number

from keepyr_utils import round_down_to_nearest, decode_name


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def place_resting_orders(
    drift_client: DriftClient,
    perp_market_account: PerpMarketAccount,
    oracle_price_data: OraclePriceData,
    mark_price: int,
):
    mark_offset = mark_price - oracle_price_data.price

    await drift_client.cancel_orders(
        MarketType.Perp(),
        perp_market_account.market_index,
    )

    now = time.time()

    params = [
        OrderParams(
            market_index=perp_market_account.market_index,
            order_type=OrderType.Limit(),
            direction=PositionDirection.Long(),
            market_type=MarketType.Perp(),
            base_asset_amount=perp_market_account.amm.order_step_size * 5,
            oracle_price_offset=mark_offset
            - (perp_market_account.amm.order_tick_size * 15),
            post_only=PostOnlyParams.TryPostOnly(),
            max_ts=int(now) + (60 * 5),
        ),
        OrderParams(
            market_index=perp_market_account.market_index,
            order_type=OrderType.Limit(),
            direction=PositionDirection.Short(),
            market_type=MarketType.Perp(),
            base_asset_amount=perp_market_account.amm.order_step_size * 5,
            oracle_price_offset=max(
                PRICE_PRECISION // 150,
                mark_offset + (perp_market_account.amm.order_tick_size * 15),
            ),
            post_only=PostOnlyParams.TryPostOnly(),
        ),
    ]

    await drift_client.place_orders(params)


def calculate_base_amount_to_mm(
    perp_market_account: PerpMarketAccount,
    net_spot_market_value: int,
    target_leverage: float = 1,
):
    base_price_normalized = convert_to_number(
        perp_market_account.amm.historical_oracle_data.last_oracle_price_twap
    )

    base_price_normalized_tick = base_price_normalized
    while base_price_normalized_tick > 100:
        base_price_normalized_tick //= 10

    tc_normalized = min(
        round_down_to_nearest(
            convert_to_number(net_spot_market_value, QUOTE_PRECISION),
            base_price_normalized_tick,
        ),
        800_000,  # hard coded limit
    )

    logger.info(f" {net_spot_market_value} -> {tc_normalized}")

    target_leverage *= 0.95
    market_symbol = decode_name(perp_market_account.name)

    max_base = (tc_normalized // base_price_normalized) * target_leverage

    logger.info(
        f"(mkt index: {market_symbol}) base to market make (targetLvg={target_leverage}): "
        f"{max_base} = {tc_normalized} / {base_price_normalized} * {target_leverage}"
    )

    return max_base


def is_market_volatile(
    perp_market_account: PerpMarketAccount,
    oracle_price_data: OraclePriceData,
    volatile_threshold: float = 0.005,
):
    twap_price = (
        perp_market_account.amm.historical_oracle_data.last_oracle_price_twap5min
    )
    last_price = perp_market_account.amm.historical_oracle_data.last_oracle_price
    current_price = oracle_price_data.price

    min_denom = min(current_price, last_price, twap_price)

    # Calculate current vs last and current vs twap, scaled by PRICE_PRECISION
    c_vs_l = abs((current_price - last_price) * PRICE_PRECISION // min_denom)
    c_vs_t = abs((current_price - twap_price) * PRICE_PRECISION // min_denom)

    recent_std = perp_market_account.amm.oracle_std * PRICE_PRECISION // min_denom

    # Convert the values to percentages, scaled by PERCENTAGE_PRECISION
    c_vs_l_percentage = c_vs_l / PERCENTAGE_PRECISION
    c_vs_t_percentage = c_vs_t / PERCENTAGE_PRECISION
    recent_std_percentage = recent_std / PERCENTAGE_PRECISION

    # Compare with volatile threshold
    return (
        recent_std_percentage > volatile_threshold
        or c_vs_t_percentage > volatile_threshold
        or c_vs_l_percentage > volatile_threshold
    )
