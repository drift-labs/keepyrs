import time
import logging

from typing import Tuple

from driftpy.dlob.dlob import DLOB, NodeToTrigger, NodeToFill, DLOBNode
from driftpy.types import MarketType, PerpMarketAccount, is_variant
from driftpy.math.orders import (
    is_order_expired,
    is_fillable_by_vamm,
    calculate_base_asset_amount_for_amm_to_fulfill,
)
from driftpy.math.oracles import is_oracle_valid

from keepyr_utils import (
    get_node_to_fill_signature,
    get_node_to_trigger_signature,
    get_fill_signature_from_user_account_and_order_id,
)

from perp_filler.src.utils import *
from perp_filler.src.constants import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_still_throttled(perp_filler, key: str) -> bool:
    last_fill_attempt = perp_filler.throttled_nodes.get(key) or 0
    if last_fill_attempt + (FILL_ORDER_THROTTLE_BACKOFF // 1_000) > time.time():
        return True
    else:
        remove_throttled_node(perp_filler, key)
        return False


def is_node_throttled(perp_filler, node: DLOBNode) -> bool:
    if (not node.user_account) or (not node.order):  # type: ignore
        return False

    if node.user_account in perp_filler.throttled_nodes:  # type: ignore
        if is_still_throttled(perp_filler, node.user_account):  # type: ignore
            return True
        else:
            return False

    order_sig = get_fill_signature_from_user_account_and_order_id(node.user_account, node.order.order_id)  # type: ignore

    if order_sig in perp_filler.throttled_nodes:
        if is_still_throttled(perp_filler, order_sig):
            return True
        else:
            return False

    return False


def get_perp_nodes_for_market(
    perp_filler,
    market: PerpMarketAccount,
    dlob: DLOB,
) -> Tuple[list[NodeToFill], list[NodeToTrigger]]:
    market_idx = market.market_index

    oracle_price_data = perp_filler.drift_client.get_oracle_price_data_for_perp_market(
        market_idx
    )

    v_ask = calculate_ask_price(market, oracle_price_data)  # type: ignore
    v_bid = calculate_bid_price(market, oracle_price_data)  # type: ignore

    fill_slot = get_latest_slot(perp_filler)

    nodes_to_fill: list[NodeToFill] = dlob.find_nodes_to_fill(
        market_idx,
        fill_slot,
        int(time.time()),
        MarketType.Perp(),
        oracle_price_data,  # type: ignore
        self.drift_client.get_state_account(),  # type: ignore
        self.drift_client.get_perp_market_account(market_idx),  # type: ignore
        v_bid,
        v_ask,
    )

    nodes_to_trigger: list[NodeToTrigger] = dlob.find_nodes_to_trigger(
        market_idx,
        oracle_price_data.price,  # type: ignore
        MarketType.Perp(),
        self.drift_client.get_state_account(),  # type: ignore
    )

    return (nodes_to_fill, nodes_to_trigger)


def filter_perp_nodes_for_market(
    perp_filler,
    fillable: list[NodeToFill],
    triggerable: list[NodeToTrigger],
) -> Tuple[list[NodeToFill], list[NodeToTrigger]]:
    seen_fillable: set[str] = set()
    filtered_fillable: list[NodeToFill] = []

    for fillable_node in fillable:
        sig = get_node_to_fill_signature(fillable_node)
        if sig not in seen_fillable:
            seen_fillable.add(sig)
            if filter_fillable(perp_filler, fillable_node):
                filtered_fillable.append(fillable_node)

    seen_triggerable: set[str] = set()
    filtered_triggerable: list[NodeToTrigger] = []

    for triggerable_node in triggerable:
        sig = get_node_to_trigger_signature(triggerable_node)
        if sig not in seen_triggerable:
            seen_triggerable.add(sig)
            if filter_triggerable(perp_filler, triggerable_node):
                filtered_triggerable.append(triggerable_node)

    return (filtered_fillable, filtered_triggerable)


def filter_fillable(perp_filler, node: NodeToFill) -> bool:
    if not node.node.order:  # type: ignore
        return False

    market_index = node.node.order.market_index  # type: ignore
    market_type = node.node.order.market_type  # type: ignore
    user_account = node.node.user_account  # type: ignore
    order_id = node.node.order.order_id  # type: ignore

    if node.node.is_vamm_node():
        logger.warning(
            f"filtered out a vAMM node on market: {market_index} for user {user_account}-{order_id}"
        )
        return False

    if node.node.have_filled:  # type: ignore
        logger.warning(
            f"filtered out a filled node on market: {market_index} for user {user_account}-{order_id}"
        )
        return False

    now = time.time()
    node_sig = get_node_to_fill_signature(node)
    if node_sig in perp_filler.filling_nodes:
        time_started = perp_filler.filling_nodes.get(node_sig) or 0
        if time_started + (FILL_ORDER_BACKOFF // 1_000) > now:
            # still cooling down
            return False

    if is_node_throttled(perp_filler, node.node):
        return False

    oracle_price_data = perp_filler.drift_client.get_oracle_price_data_for_perp_market(
        market_index
    )

    if is_order_expired(node.node.order, int(time.time())):  # type: ignore
        if is_variant(node.node.order.order_type, "Limit"):  # type: ignore
            # do not try to fill limit orders because they will auto expire when filled against
            return False
        logger.warning(
            f"order is expired on market: {market_index}"
            f"for user {user_account}-{order_id}"
        )
        return True

    is_maker_empty = len(node.maker) == 0
    is_perp_market_type = is_variant(market_type, "Perp")

    perp_market_account = perp_filler.drift_client.get_perp_market_account(market_index)
    latest_slot = get_latest_slot(perp_filler)
    ts = int(time.time())
    state_account = perp_filler.drift_client.get_state_account()

    is_fillable = is_fillable_by_vamm(
        node.node.order,  # type: ignore
        perp_market_account,  # type: ignore
        oracle_price_data,  # type: ignore
        latest_slot,
        ts,
        state_account.min_perp_auction_duration,  # type: ignore
    )

    if is_maker_empty and is_perp_market_type and not is_fillable:
        logger.warning(
            f"filtered out unfillable node on market {market_index} for user {user_account}-{order_id}"
        )
        logger.warning(f" . no maker node: {len(node.maker) == 0}")
        logger.warning(f" . is perp: {is_variant(market_type, 'perp')}")
        logger.warning(f" . is not fillable by vamm: {not is_fillable}")

        base_fulfilled = calculate_base_asset_amount_for_amm_to_fulfill(
            node.node.order,  # type: ignore
            perp_market_account,  # type: ignore
            oracle_price_data,  # type: ignore
            get_latest_slot(perp_filler),
        )
        logger.warning(
            f" . calculate_base_asset_amount_for_amm_to_fulfill: {base_fulfilled}"
        )
        return False

    if is_maker_empty:
        oracle_valid = is_oracle_valid(
            perp_market_account.amm,  # type: ignore
            oracle_price_data,  # type: ignore
            state_account.oracle_guard_rails,  # type: ignore
            get_latest_slot(perp_filler),
        )
        if not oracle_valid:
            logger.error(f"Oracle is not valid for market: {market_index}")
            return False

    return True


def filter_triggerable(perp_filler, node: NodeToTrigger) -> bool:
    # TODO
    if node.node.have_trigger:
        return False

    now = time.time()
    sig = get_node_to_trigger_signature(node)
    time_started = perp_filler.triggering_nodes.get(sig)
    if time_started:
        if time_started + (TRIGGER_ORDER_COOLDOWN_MS / 1_000) > now:
            return False

    return True
