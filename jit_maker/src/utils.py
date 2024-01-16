import logging

from driftpy.types import (
    SpotMarketAccount,
    PerpMarketAccount,
    OraclePriceData,
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


def calculate_base_amount_to_mm_perp(
    perp_market_account: PerpMarketAccount,
    net_spot_market_value: int,
    target_leverage: float = 1,
):
    base_price_normalized = convert_to_number(
        perp_market_account.amm.historical_oracle_data.last_oracle_price_twap
    )

    tc_normalized = convert_to_number(net_spot_market_value, QUOTE_PRECISION)

    logger.info(f"{net_spot_market_value} -> {tc_normalized}")

    target_leverage *= 0.95
    market_symbol = decode_name(perp_market_account.name)

    max_base = (tc_normalized / base_price_normalized) * target_leverage

    logger.info(
        f"(mkt index: {market_symbol}) base to market make (targetLvg={target_leverage}): "
        f"{max_base} = {tc_normalized} / {base_price_normalized} * {target_leverage}"
    )

    return max_base


def calculate_base_amount_to_mm_spot(
    spot_market_account: SpotMarketAccount,
    net_spot_market_value: int,
    target_leverage: float = 1,
):
    base_price_normalized = convert_to_number(
        spot_market_account.historical_oracle_data.last_oracle_price_twap
    )

    tc_normalized = convert_to_number(net_spot_market_value, QUOTE_PRECISION)

    logger.info(f" {net_spot_market_value} -> {tc_normalized}")

    target_leverage *= 0.95
    market_symbol = decode_name(spot_market_account.name)

    max_base = (tc_normalized / base_price_normalized) * target_leverage

    logger.info(
        f"(mkt index: {market_symbol}) base to market make (targetLvg={target_leverage}): "
        f"{max_base} = {tc_normalized} / {base_price_normalized} * {target_leverage}"
    )

    return max_base


def is_perp_market_volatile(
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


def is_spot_market_volatile(
    spot_market_account: SpotMarketAccount,
    oracle_price_data: OraclePriceData,
    volatile_threshold: float = 0.005,
):
    twap_price = spot_market_account.historical_oracle_data.last_oracle_price_twap5min
    last_price = spot_market_account.historical_oracle_data.last_oracle_price
    current_price = oracle_price_data.price

    min_denom = min(current_price, last_price, twap_price)

    # Calculate current vs last and current vs twap, scaled by PRICE_PRECISION
    c_vs_l = abs((current_price - last_price) * PRICE_PRECISION // min_denom)
    c_vs_t = abs((current_price - twap_price) * PRICE_PRECISION // min_denom)

    # Convert the values to percentages, scaled by PERCENTAGE_PRECISION
    c_vs_l_percentage = c_vs_l / PERCENTAGE_PRECISION
    c_vs_t_percentage = c_vs_t / PERCENTAGE_PRECISION

    # Compare with volatile threshold
    return (
        c_vs_t_percentage > volatile_threshold or c_vs_l_percentage > volatile_threshold
    )
