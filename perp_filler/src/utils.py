import logging
import time

from typing import Optional
from dataclasses import dataclass

from solders.instruction import Instruction
from solders.pubkey import Pubkey

from driftpy.dlob.dlob import NodeToFill, NodeToTrigger, DLOB
from driftpy.types import (
    UserAccount,
    MakerInfo,
    UserStatsAccount,
    ReferrerInfo,
    MarketType,
)
from driftpy.constants import *
from driftpy.math.conversion import convert_to_number

from keepyr_utils import get_node_to_fill_signature, get_node_to_trigger_signature

from perp_filler.src.constants import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_latest_slot(perp_filler) -> int:
    return max(perp_filler.slot_subscriber.get_slot(), perp_filler.user_map.latest_slot)


def remove_throttled_node(perp_filler, sig: str):
    del perp_filler.throttled_nodes[sig]


def remove_triggering_node(perp_filler, node: NodeToTrigger):
    del perp_filler.triggering_nodes[get_node_to_trigger_signature(node)]


def remove_filling_nodes(perp_filler, nodes: list[NodeToFill]):
    for node in nodes:
        del perp_filler.filling_nodes[get_node_to_fill_signature(node)]


def set_throttled_node(perp_filler, sig: str):
    perp_filler.throttled_nodes[sig] = int(time.time())


def prune_throttled_nodes(perp_filler):
    nodes_to_prune = {}

    if len(perp_filler.throttled_nodes) > THROTTLED_NODE_SIZE_TO_PRUNE:
        for key, val in perp_filler.throttled_nodes:
            if val + 2 * (FILL_ORDER_BACKOFF // 1_000) > time.time():
                nodes_to_prune[key] = val

        for key, val in nodes_to_prune:
            perp_filler.remove_throttled_node(key)


def get_dlob(perp_filler) -> DLOB:
    return perp_filler.dlob_subscriber.get_dlob()


def log_slots(perp_filler):
    logger.info(
        f"slot_subscriber slot: {perp_filler.slot_subscriber.get_slot()} user_map slot: {perp_filler.user_map.get_slot()}"
    )


async def get_user_account_from_map(perp_filler, key: str) -> UserAccount:
    return (await perp_filler.user_map.must_get(key)).get_user_account()


def calc_compact_u16_encoded_size(array, elem_size: int = 1):
    """
    Returns the number of bytes occupied by this array if it were serialized in compact-u16-format.
    NOTE: assumes each element of the array is 1 byte (not sure if this holds?)

    See Solana documentation on compact-u16 format:
    https://docs.solana.com/developing/programming-model/transactions#compact-u16-format

    For more information:
    https://stackoverflow.com/a/69951832

    Example mappings from hex to compact-u16:
      hex     |  compact-u16
      --------+------------
      0x0000  |  [0x00]
      0x0001  |  [0x01]
      0x007f  |  [0x7f]
      0x0080  |  [0x80 0x01]
      0x3fff  |  [0xff 0x7f]
      0x4000  |  [0x80 0x80 0x01]
      0xc000  |  [0x80 0x80 0x03]
      0xffff  |  [0xff 0xff 0x03]
    """
    if len(array) > 0x3FFF:
        return 3 + len(array) * elem_size
    elif len(array) > 0x7F:
        return 2 + len(array) * elem_size
    else:
        return 1 + (len(array) * elem_size or 1)


def calc_ix_encoded_size(ix: Instruction) -> int:
    accounts = [None] * len(ix.accounts)
    data = [None] * len(ix.data)
    return (
        1
        + calc_compact_u16_encoded_size(accounts, 1)
        + calc_compact_u16_encoded_size(data, 1)
    )


def log_message_for_node_to_fill(node: NodeToFill, prefix: Optional[str]) -> str:
    taker = node.node
    order = getattr(taker, "order")

    if not order:
        return "no taker order"

    msg = ""

    msg += (
        f"taker on market {order.market_index}: {taker.user_account}-"  # type: ignore
        f"{order.order_id} {str(order.direction)} "  # type: ignore
        f"{convert_to_number(order.base_asset_amount_filled, BASE_PRECISION)}/"  # type: ignore
        f"{convert_to_number(order.base_asset_amount, BASE_PRECISION)} @ "  # type: ignore
        f"{convert_to_number(order.price, PRICE_PRECISION)} "  # type: ignore
        f"(orderType: {str(order.order_type)})\n"
    )  # type: ignore

    msg += "makers:\n"

    if len(node.maker) > 0:
        for idx, _ in enumerate(node.maker):
            maker = node.maker[idx]
            order = getattr(maker, "order")

            msg += (
                f"  [{i}] market {order.market_index}: {maker.user_account}-{order.order_id} "  # type: ignore
                f"{str(order.direction)} "  # type: ignore
                f"{convert_to_number(order.base_asset_amount_filled, BASE_PRECISION)}/"  # type: ignore
                f"{convert_to_number(order.base_asset_amount, BASE_PRECISION)} @ "  # type: ignore
                f"{convert_to_number(order.price, PRICE_PRECISION)} (offset: "  # type: ignore
                f"{order.oracle_price_offset / PRICE_PRECISION} "  # type: ignore
                f"(orderType: {str(order.order_type)})\n"
            )  # type: ignore
    else:
        msg += "  vAMM"

    return msg
