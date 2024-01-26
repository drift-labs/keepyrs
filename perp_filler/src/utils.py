import logging
import time

from typing import Optional

from solders.instruction import Instruction
from solders.pubkey import Pubkey
from solders.rpc.responses import GetSlotResp

from driftpy.dlob.dlob import NodeToFill, NodeToTrigger, DLOB
from driftpy.types import UserAccount
from driftpy.constants import *
from driftpy.math.conversion import convert_to_number

from keepyr_utils import (
    get_fill_signature_from_user_account_and_order_id,
    get_node_to_fill_signature,
    get_node_to_trigger_signature,
)
from keepyr_parse import (
    is_end_ix_log,
    is_fill_ix_log,
    is_ix_log,
    is_order_does_not_exist_log,
    is_taker_breached_maintenance_margin_log,
    is_maker_breached_maintenance_margin_log,
    is_err_filling_log,
    is_err_stale_oracle,
)

from perp_filler.src.constants import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_latest_slot(perp_filler) -> int:
    slot_sub_slot = perp_filler.slot_subscriber.get_slot()
    if isinstance(
        slot_sub_slot, GetSlotResp
    ):  # i don't know why this sometimes happens
        return max(slot_sub_slot.value, perp_filler.user_map.get_slot())
    else:
        return max(
            perp_filler.slot_subscriber.get_slot(), perp_filler.user_map.get_slot()
        )


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
        for key, val in perp_filler.throttled_nodes.items():
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
    return (await perp_filler.user_map.must_get(str(key))).get_user_account()


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

    if prefix:
        msg += prefix + " "

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
                f"  [{idx}] market {order.market_index}: {maker.user_account}-{order.order_id} "  # type: ignore
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


async def handle_transaction_logs(
    perp_filler, nodes: list[NodeToFill], logs: Optional[list[str]] = None
):
    if not logs:
        return 0

    in_fill_ix = False
    error_this_fill_ix = False
    ix_idx = -1  # skip cu budget program
    success_count = 0
    bursted = False
    for log in logs:
        if not log:
            logger.error("Log is None")
            continue

        if "exceeded maximum number of instructions allowed" in log:
            # switch to burst cu limit temporarily
            perp_filler.use_burst_cu_limit = True
            perp_filler.fill_tx_since_burst_cu = 0
            bursted = True
            continue

        if is_end_ix_log(str(perp_filler.drift_client.program_id), log):
            if not error_this_fill_ix:
                success_count += 1

                in_fill_ix = False
                error_this_fill_ix = False
                continue

        if is_ix_log(log):
            if is_fill_ix_log(log):
                in_fill_ix = True
                error_this_fill_ix = False
                ix_idx += 1
                if ix_idx < len(nodes):
                    node = nodes[ix_idx]
                    logger.info(
                        log_message_for_node_to_fill(
                            node, f"Processing tx log for assoc node: {ix_idx}"
                        )
                    )
                else:
                    ix_idx -= 1
                    continue
            else:
                in_fill_ix = False
            continue

        if not in_fill_ix:
            # this is not a log for a fill ix
            continue

        order_dne = is_order_does_not_exist_log(log)
        if order_dne:
            node = nodes[ix_idx]
            if node:
                logger.error(
                    f"assoc node (ix idx: {ix_idx}): "
                    f"{str(node.node.user_account)}-{node.node.order.order_id} "  # type: ignore
                    f"dne, already filled"
                )
                set_throttled_node(perp_filler, get_node_to_fill_signature(node))
                error_this_fill_ix = True
                continue

        maker_breached = is_maker_breached_maintenance_margin_log(log)
        if maker_breached:
            logger.error(
                f"Throttling maker breached maint. mrgn: {maker_breached} ix idx: {ix_idx}"
            )
            set_throttled_node(perp_filler, maker_breached)
            maker_pubkey = Pubkey.from_string(maker_breached)
            user_account = await get_user_account_from_map(perp_filler, maker_breached)
            try:
                tx_sig = await perp_filler.drift_client.force_cancel_orders(
                    maker_pubkey, user_account
                )
                logger.info(
                    f"Force cancelled orders for maker {user_account} due to breach of maint. mrgn"
                )
                logger.info(f"tx sig: {tx_sig}")
            except Exception as e:
                logger.error(f"Failed to force cancel orders for maker: {node.node.user_account}")  # type: ignore
                logger.error(f"Error: {e}")

            error_this_fill_ix = True
            break

        taker_breached = is_taker_breached_maintenance_margin_log(log)
        if taker_breached and nodes[ix_idx]:
            node = nodes[ix_idx]
            taker_node_sig = node.node.user_account  # type: ignore
            user_account = await get_user_account_from_map(perp_filler, taker_node_sig)
            logger.error(
                f"Throttling taker breached maint. mrgn: {taker_node_sig} ix idx: {ix_idx}"
            )
            set_throttled_node(perp_filler, taker_node_sig)
            try:
                tx_sig = await perp_filler.drift_client.force_cancel_orders(
                    Pubkey.from_string(taker_node_sig), user_account
                )
                logger.info(
                    f"Force cancelled orders for maker {user_account} due to breach of maint. mrgn"
                )
                logger.info(f"tx sig: {tx_sig}")
            except Exception as e:
                logger.error(f"Failed to force cancel orders for taker: {node.node.user_account}")  # type: ignore
                logger.error(f"Error: {e}")
            error_this_fill_ix = True
            continue

        err_filling_log = is_err_filling_log(log)
        if err_filling_log:
            order_id = err_filling_log[0]
            user_acc = err_filling_log[1]
            extracted_sig = get_fill_signature_from_user_account_and_order_id(
                user_acc, order_id
            )
            set_throttled_node(perp_filler, extracted_sig)

            node = nodes[ix_idx]
            assoc_sig = get_node_to_fill_signature(node)
            logger.warning(
                f"throttling node due to fill error. extracted sig: {extracted_sig} assoc. node sig: {assoc_sig} node idx: {ix_idx}"
            )
            error_this_fill_ix = True
            continue

        if is_err_stale_oracle(log):
            logger.error(f"stale oracle error: {log}")
            error_this_fill_ix = True
            continue

    if not bursted:
        if perp_filler.fill_tx_since_burst_cu > TX_COUNT_COOLDOWN_ON_BURST:
            perp_filler.use_burst_cu_limit = False
        perp_filler.fill_tx_since_burst_cu += 1

    return success_count
