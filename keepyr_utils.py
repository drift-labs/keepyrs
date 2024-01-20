import math

from typing import Optional

from driftpy.dlob.dlob import DLOB, NodeToFill, NodeToTrigger
from driftpy.dlob.dlob_node import DLOBNode
from driftpy.dlob.node_list import get_order_signature
from driftpy.types import OraclePriceData, MarketType


def get_best_limit_bid_exclusionary(
    dlob: DLOB,
    market_index: int,
    market_type: MarketType,
    slot: int,
    oracle_price_data: OraclePriceData,
    excluded_pubkey: str,
    excluded_user_accounts_and_order: list[tuple[str, int]] = [],
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


def get_node_to_fill_signature(node: NodeToFill) -> str:
    if not node.node.user_account:  # type: ignore
        return "~"

    return f"{node.node.user_account}-{node.node.order.order_id}"  # type: ignore


def get_node_to_trigger_signature(node: NodeToTrigger) -> str:
    return get_order_signature(node.node.order.order_id, node.node.user_account)  # type: ignore


def get_fill_signature_from_user_account_and_order_id(
    user_account: str, order_id: int
) -> str:
    return f"{user_account}-{order_id}"
