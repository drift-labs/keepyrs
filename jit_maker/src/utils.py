import asyncio
import base64
import math
import logging
import traceback
import requests

from typing import Optional

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
from solders.instruction import Instruction, AccountMeta
from solders.pubkey import Pubkey
from solders.address_lookup_table_account import AddressLookupTableAccount

from driftpy.drift_client import DriftClient
from driftpy.drift_user import DriftUser
from driftpy.types import (
    is_variant,
    PerpMarketAccount,
    OraclePriceData,
    PositionDirection,
)
from driftpy.constants.numeric_constants import (
    PRICE_PRECISION,
    QUOTE_PRECISION,
    PERCENTAGE_PRECISION,
    BASE_PRECISION,
)
from driftpy.math.conversion import convert_to_number
from driftpy.math.spot_position import get_signed_token_amount, get_token_amount
from driftpy.address_lookup_table import get_address_lookup_table

from keepyr_utils import round_down_to_nearest, decode_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

JUPITER_URL = "https://quote-api.jup.ag/v6"
JUPITER_SLIPPAGE_BPS = 10
# this is the slippage away from oracle that we're willing to tolerate
JUPITER_ORACLE_SLIPPAGE_BPS = 50


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

    max_base = (tc_normalized / base_price_normalized) * target_leverage

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


async def rebalance(
    drift_client: DriftClient,
    user: DriftUser,
    perp_idx: int,
    spot_idx: int,
    max_dollar_size: int = 0,
    base_delta_b4_hedge: float = 0,
    lookup_tables: Optional[list] = None,
):
    perp_market_account = drift_client.get_perp_market_account(perp_idx)
    spot_market_account = drift_client.get_spot_market_account(spot_idx)

    user_spot_position = user.get_spot_position(spot_idx)

    if perp_market_account is None or spot_market_account is None:
        logger.error(f"perp market {perp_idx} or spot market {spot_idx} not found")
        raise ValueError(f"perp market {perp_idx} or spot market {spot_idx} not found")

    assert str(perp_market_account.amm.oracle) == str(spot_market_account.oracle)

    perp_size = user.get_perp_position_with_lp_settle(perp_idx)[0].base_asset_amount

    spot_size = 0

    if user_spot_position is not None:
        spot_size = get_signed_token_amount(
            get_token_amount(
                user_spot_position.scaled_balance,
                spot_market_account,
                user_spot_position.balance_type,
            ),
            user_spot_position.balance_type,
        )

    spot_precision = 10**spot_market_account.decimals
    spot_size_num = convert_to_number(spot_size, spot_precision)
    perp_size_num = convert_to_number(perp_size, BASE_PRECISION)
    mismatch = perp_size_num + spot_size_num
    mismatch_threshold = base_delta_b4_hedge

    last_oracle_price = convert_to_number(
        perp_market_account.amm.historical_oracle_data.last_oracle_price,
        PRICE_PRECISION,
    )

    if (
        abs(mismatch) > abs(mismatch_threshold)
        and abs(mismatch * last_oracle_price) > 10
    ):
        direction = (
            PositionDirection.Long() if mismatch < 0 else PositionDirection.Short()
        )

        trade_size = abs(mismatch) * BASE_PRECISION

        if max_dollar_size != 0:
            trade_size = min(
                max_dollar_size
                // (
                    perp_market_account.amm.historical_oracle_data.last_oracle_price
                    // 1e6
                )
                * BASE_PRECISION,
                trade_size,
            )

            trade_size_usd = convert_to_number(
                trade_size
                * perp_market_account.amm.historical_oracle_data.last_oracle_price
                // BASE_PRECISION,
                PRICE_PRECISION,
            )

            if perp_idx != 0:
                trade_size //= 10

            try:
                instructions = await spot_hedge(
                    spot_idx,
                    drift_client,
                    trade_size,
                    (trade_size_usd * QUOTE_PRECISION * 1.001),
                    direction,
                    last_oracle_price,
                )
                if instructions:
                    drift_lookup_tables = (
                        lookup_tables if lookup_tables is not None else []
                    )
                    lookup_tables = drift_lookup_tables + instructions[1]
                    if lookup_tables is None:
                        lookup_tables = []
                    sig = await send_rebalance_tx(
                        drift_client, instructions[0], lookup_tables
                    )
                    if sig:
                        logger.info(f"rebalance tx sig: https://solscan.io/tx/{sig}")

            except Exception as e:
                logger.error(f"Error hedging: {e}")


async def spot_hedge(
    spot_idx: int,
    drift_client: DriftClient,
    trade_size: int,
    trade_size_usd: int,
    direction: PositionDirection,
    oracle_price: int,
):
    if is_variant(direction, "Long"):
        # sell usdc, buy spot_idx
        in_market_idx = 0
        out_market_idx = spot_idx
        size = trade_size
    else:
        # sell spot_idx, buy usdc
        in_market_idx = spot_idx
        out_market_idx = 0
        size = trade_size_usd

    in_market = drift_client.get_spot_market_account(in_market_idx)
    out_market = drift_client.get_spot_market_account(out_market_idx)

    if in_market is None or out_market is None:
        raise ValueError(
            f"in_market {in_market_idx} or out_market {out_market_idx} not found"
        )

    in_market_precision = 10**in_market.decimals
    out_market_precision = 10**out_market.decimals

    logger.info(
        f"Jupiter swap: {str(direction)}: {size}, in_market: {in_market_idx}, out_market: {out_market_idx}"
    )

    url = f"{JUPITER_URL}/quote?inputMint={str(in_market.mint)}&outputMint={str(out_market.mint)}&amount={math.floor(size)}&slippageBps={JUPITER_SLIPPAGE_BPS}"

    quote_resp = requests.get(url)

    if quote_resp.status_code != 200:
        logger.error(
            f"failed to get quote with params inputMint: {in_market.mint}, outputMint: {out_market.mint}, amount: {int(size)}, slippageBps: {JUPITER_SLIPPAGE_BPS}"
        )
        return None

    quote = quote_resp.json()

    in_amount_num = convert_to_number(int(quote["inAmount"]), in_market_precision)
    out_amount_num = convert_to_number(int(quote["outAmount"]), out_market_precision)

    if is_variant(direction, "Long"):
        # in usdc, out spot
        # swap price = in / out
        swap_price = in_amount_num / out_amount_num

        # tolerable buys are JUPITER_SLIPPAGE_BPS above oracle price
        tolerable_price = swap_price < oracle_price * (
            1 + JUPITER_ORACLE_SLIPPAGE_BPS / 10_000
        )
        from_oracle_bps = (swap_price / oracle_price - 1) * 10_000
    else:
        # in spot, out usdc
        # swap price = out / in
        swap_price = out_amount_num / in_amount_num

        # tolerable buys are JUPITER_SLIPPAGE_BPS below oracle price
        tolerable_price = swap_price > oracle_price * (
            1 - JUPITER_ORACLE_SLIPPAGE_BPS / 10_000
        )
        from_oracle_bps = (swap_price / oracle_price - 1) * 10_000

    if not tolerable_price:
        logger.warning(
            f"Not swapping spot markets {in_market_idx} -> {out_market_idx}, "
            f"amounts {in_amount_num} -> {out_amount_num}, swapPrice: {swap_price}, "
            f"oracle: {oracle_price} (fromOracle: {from_oracle_bps} bps), decent ?: {tolerable_price}"
        )
        return None
    else:
        logger.info(
            f"Swapping spot markets {in_market_idx} -> {out_market_idx}, "
            f"amounts {in_amount_num} -> {out_amount_num}, swapPrice: {swap_price}, "
            f"oracle: {oracle_price} (fromOracle: {from_oracle_bps} bps), decent ?: {tolerable_price}"
        )

        return await drift_client.get_jupiter_swap_ix_v6(
            out_market_idx,
            in_market_idx,
            amount=math.floor(size),
            quote=quote,
            slippage_bps=JUPITER_SLIPPAGE_BPS,
        )


async def send_rebalance_tx(
    drift_client: DriftClient,
    instructions: list,
    lookup_tables: list,
):
    cu_estimate = 2_000_000

    set_cu_limit_ix = set_compute_unit_limit(cu_estimate)
    set_cu_price_ix = set_compute_unit_price(math.floor(1000 // (cu_estimate * 1e-6)))

    ixs: list[Instruction] = [set_cu_limit_ix, set_cu_price_ix]

    for instruction in instructions:
        if type(instruction) is list:
            continue
        elif type(instruction) is dict:
            ix = dict_to_instructions(instruction)
            ixs.append(ix)
        else:
            ixs.append(instruction)

    lookup_table_accounts: list[AddressLookupTableAccount] = []
    for table in lookup_tables:
        lookup_table_account = await get_address_lookup_table(
            drift_client.connection, Pubkey.from_string(table)
        )
        lookup_table_accounts.append(lookup_table_account)

    try:
        try:
            versioned_tx = await asyncio.wait_for(
                drift_client.tx_sender.get_versioned_tx(
                    ixs=ixs,
                    payer=drift_client.wallet.payer,
                    lookup_tables=lookup_table_accounts,
                    additional_signers=None,
                ),
                timeout=10,
            )
        except asyncio.TimeoutError:
            logger.error("Timed out getting versioned tx for tx chunk")
            return None

        try:
            sig = await asyncio.wait_for(
                drift_client.tx_sender.send(versioned_tx),
                timeout=5,
            )

            return sig.tx_sig
        except asyncio.TimeoutError:
            logger.error("Timed out sending versioned transaction")
            return None
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Failed to send rebalance tx: {e}\n{tb}")


def list_to_instructions(instructions_list: list) -> list[Instruction]:
    instructions = []

    for item in instructions_list:
        program_id = Pubkey.from_string(item["programId"])
        accounts = [
            AccountMeta(
                Pubkey.from_string(account["pubkey"]),
                account["isSigner"],
                account["isWritable"],
            )
            for account in item["accounts"]
        ]
        data = base64.b64decode(item["data"])
        instruction = Instruction(program_id, data, accounts)
        instructions.append(instruction)

    return instructions


def dict_to_instructions(instructions_dict: dict) -> Instruction:
    program_id = Pubkey.from_string(instructions_dict["programId"])
    accounts = [
        AccountMeta(
            Pubkey.from_string(account["pubkey"]),
            account["isSigner"],
            account["isWritable"],
        )
        for account in instructions_dict["accounts"]
    ]
    data = base64.b64decode(instructions_dict["data"])
    return Instruction(program_id, data, accounts)
