import logging
import math
import time

from typing import Set

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price

from driftpy.dlob.dlob import NodeToFill, NodeToTrigger
from driftpy.types import is_variant

from keepyr_utils import COMPUTE_BUDGET_PROGRAM

from perp_filler.src.constants import *
from perp_filler.src.utils import (
    calc_compact_u16_encoded_size,
    calc_ix_encoded_size,
    get_node_fill_info,
    log_slots,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def execute_fillable_perp_nodes_for_market(perp_filler, nodes: list[NodeToFill]):
    logger.info(f"filling {len(nodes)} nodes")


async def execute_triggerable_perp_nodes_for_market(
    perp_filler, nodes: list[NodeToTrigger]
):
    logger.info(f"filling {len(nodes)} nodes")


async def try_bulk_fill_perp_nodes(perp_filler, nodes: list[NodeToFill]):
    micro_lamports = min(
        math.floor(perp_filler.priority_fee_subscriber.max_priority_fee),
        MAX_CU_PRICE_MICRO_LAMPORTS,
    )
    ixs = [
        set_compute_unit_limit(MAX_CU_PER_TX),
        set_compute_unit_price(micro_lamports),
    ]

    """
    At all times, the running transaction size consists of:
    - Signatures: A compact-u16 array with 64 bytes per element.
    - Message header: 3 bytes.
    - Affected accounts: A compact-u16 array with 32 bytes per element.
    - Previous block hash: 32 bytes.
    - Message instructions:
        - programIdIdx: 1 byte.
        - accountsIdx: A compact-u16 array with 1 byte per element.
        - Instruction data: A compact-u16 array with 1 byte per element.
    """

    running_tx_size = 0
    running_cu_used = 0

    unique_accounts: Set[str] = set()
    unique_accounts.add(str(perp_filler.drift_client.wallet.pubkey()))

    compute_budget_ix = ixs[0]
    for account in compute_budget_ix.accounts:
        unique_accounts.add(str(account.pubkey))
    unique_accounts.add(str(COMPUTE_BUDGET_PROGRAM))

    # initialize barebones tx
    # signatures
    running_tx_size += calc_compact_u16_encoded_size([1], 64)
    # message header
    running_tx_size += 3
    # accounts
    running_tx_size += calc_compact_u16_encoded_size([len(unique_accounts)], 32)

    # block hash
    running_tx_size += 32

    running_tx_size += calc_ix_encoded_size(compute_budget_ix)

    tx_packer_start = time.time()
    nodes_sent: list[NodeToFill] = []
    idx_used = 0
    starting_size = len(ixs)
    fill_tx_id = perp_filler.fill_tx_id + 1

    for idx, node in enumerate(nodes):
        if len(node.maker) > 1:
            # do multi maker fills in a separate tx since they're larger
            # await try_fill_multi_maker_perp_nodes(node)
            nodes_sent.append(node)
            continue
        # logger.info(log)

        log_slots(perp_filler)

        node_fill_info = await get_node_fill_info(perp_filler, node)
        maker_infos = node_fill_info.maker_infos
        taker_user_account = node_fill_info.taker_user_account
        referrer_info = node_fill_info.referrer_info
        market_type = node_fill_info.market_type

        if not is_variant(market_type, "Perp"):
            raise ValueError("Expected perp market type")
