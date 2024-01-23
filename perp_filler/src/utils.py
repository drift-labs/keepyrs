import logging
import time

from typing import Optional
from dataclasses import dataclass

from solders.instruction import Instruction
from solders.pubkey import Pubkey

from driftpy.dlob.dlob import NodeToFill, NodeToTrigger, DLOB
from driftpy.dlob.dlob_node import DLOBNode
from driftpy.types import (
    UserAccount,
    MakerInfo,
    UserStatsAccount,
    ReferrerInfo,
    MarketType,
)
from driftpy.constants import *
from driftpy.addresses import get_user_stats_account_public_key
from driftpy.math.conversion import convert_to_number
from driftpy.accounts.get_accounts import get_user_stats_account

from keepyr_utils import get_node_to_fill_signature, get_node_to_trigger_signature

from perp_filler.src.constants import *
from perp_filler.src.node_utils import is_node_throttled
from perp_filler.src.maker_utils import select_makers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MakerNodeMap = dict[str, list[DLOBNode]]


@dataclass
class NodeFillInfo:
    maker_infos: list[MakerInfo]
    taker_user_account: UserAccount
    referrer_info: ReferrerInfo
    market_type: MarketType


def get_referrer_info(
    perp_filler, taker_stats: UserStatsAccount
) -> Optional[ReferrerInfo]:
    if taker_stats.referrer == Pubkey.default():
        return None
    else:
        return ReferrerInfo(
            taker_stats.referrer,
            get_user_stats_account_public_key(
                perp_filler.drift_client.program_id, taker_stats.referrer
            ),
        )


def get_latest_slot(perp_filler) -> int:
    return max(perp_filler.slot_subscriber.get_slot(), perp_filler.user_map.get_slot())


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


async def get_node_fill_info(perp_filler, node: NodeToFill):
    maker_infos: list[MakerInfo] = []
    if len(node.maker) > 0:
        maker_nodes_map: MakerNodeMap = {}

        for maker in node.maker:
            if is_node_throttled(perp_filler, maker):
                continue

            if getattr(maker, "user_account", None) is None:
                continue

            user_account = maker.user_account  # type: ignore

            if user_account in maker_nodes_map:
                maker_nodes_map.get(user_account).append(maker)  # type: ignore
            else:
                maker_nodes_map[user_account] = [maker]

            if len(maker_nodes_map) > MAX_MAKERS_PER_FILL:
                logger.info(f"selecting from {len(maker_nodes_map)} makers")
                maker_nodes_map = select_makers(maker_nodes_map)
                logger.info(f"selected: {','.join(list(maker_nodes_map.keys()))}")

        for maker_account, nodes in maker_nodes_map.items():
            maker_node = nodes[0]

            maker_user_account = await get_user_account_from_map(
                perp_filler, maker_account
            )
            maker_authority = maker_user_account.authority
            maker_user_stats = await get_user_stats_account(
                perp_filler.drift_client.program_id, maker_authority
            )
            maker_infos.append(MakerInfo(Pubkey.from_string(maker_account), maker_user_stats, maker_user_account, maker_node.order))  # type: ignore

    taker_user_account = await get_user_account_from_map(perp_filler, str(node.node.user_account))  # type: ignore
    taker_user_stats = await get_user_stats_account(
        perp_filler.drift_client.program_id, taker_user_account.authority
    )
    referrer_info = get_referrer_info(perp_filler, taker_user_stats)

    node_fill_info = NodeFillInfo(maker_infos, taker_user_account, referrer_info, node.node.order.market_type)  # type: ignore

    return node_fill_info
