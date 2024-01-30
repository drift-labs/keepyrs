import math

from typing import Optional
from aiohttp import web

from driftpy.dlob.dlob import DLOB
from driftpy.dlob.dlob_node import DLOBNode
from driftpy.types import OraclePriceData, MarketType


def get_best_limit_bid_exclusionary(
    dlob: DLOB,
    market_index: int,
    market_type: MarketType,
    slot: int,
    oracle_price_data: OraclePriceData,
    excluded_pubkey: str,
    excluded_user_accounts_and_order: list[tuple[str, int]] = [],
    uncross: bool = False,
) -> Optional[DLOBNode]:
    bids = dlob.get_resting_limit_bids(
        market_index, slot, market_type, oracle_price_data
    )

    for bid in bids:
        if hasattr(bid, "user_account"):
            if str(bid.user_account) == excluded_pubkey:
                continue
            if hasattr(bid, "order"):
                order_id = bid.order.order_id
                price = bid.order.price
                if uncross and price > oracle_price_data.price:
                    continue
                if any(
                    entry[0] == str(bid.user_account) and entry[1] == (order_id or -1)
                    for entry in excluded_user_accounts_and_order
                ):
                    continue

        return bid

    return None


def get_best_limit_ask_exclusionary(
    dlob: DLOB,
    market_index: int,
    market_type: MarketType,
    slot: int,
    oracle_price_data: OraclePriceData,
    excluded_pubkey: str,
    excluded_user_accounts_and_order: list[tuple[str, int]] = [],
    uncross: bool = False,
) -> Optional[DLOBNode]:
    asks = dlob.get_resting_limit_asks(
        market_index, slot, market_type, oracle_price_data
    )

    for ask in asks:
        if hasattr(ask, "user_account"):
            if str(ask.user_account) == excluded_pubkey:
                continue
            if hasattr(ask, "order"):
                order_id = ask.order.order_id
                price = ask.order.price
                if uncross and price < oracle_price_data.price:
                    continue
                if any(
                    entry[0] == str(ask.user_account) and entry[1] == (order_id or -1)
                    for entry in excluded_user_accounts_and_order
                ):
                    continue

        return ask

    return None


def round_down_to_nearest(num: int, nearest: int = 100) -> int:
    if nearest == 0:
        return num  # we will just return the number if asked to round to 0
    return math.floor(num / nearest) * nearest


def decode_name(bytes_list: list[int]) -> str:
    byte_array = bytes(bytes_list)
    return byte_array.decode("utf-8").strip()


def str_to_market_type(string: str) -> MarketType:
    if string.lower() == "perp":
        return MarketType.Perp()
    elif string.lower() == "spot":
        return MarketType.Spot()
    else:
        raise ValueError(f"expected perp or spot, got {string}")


def make_health_check_handler(bot):
    async def health_check_handler(request):
        healthy = await bot.health_check()
        if healthy:
            return web.Response(status=200)  # OK status for healthy
        else:
            return web.Response(status=503)  # Service Unavailable for unhealthy

    return health_check_handler


async def start_server(bot):
    app = web.Application()
    health_handler = make_health_check_handler(bot)
    app.router.add_get(f"/health/{bot.name}", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
