import math
import time

from typing import Set

from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price  # type: ignore
from solders.instruction import Instruction  # type: ignore
from solders.transaction import VersionedTransaction  # type: ignore
from solders.pubkey import Pubkey  # type: ignore

from driftpy.dlob.dlob import NodeToFill, NodeToTrigger
from driftpy.types import is_variant, MakerInfo, Order
from driftpy.tx.types import TxSigAndSlot
from driftpy.addresses import get_user_account_public_key

from keepyr_utils import (
    COMPUTE_BUDGET_PROGRAM,
    get_node_to_fill_signature,
    get_node_to_trigger_signature,
    simulate_and_get_tx_with_cus,
)

from perp_filler.src.constants import *
from perp_filler.src.utils import (
    calc_compact_u16_encoded_size,
    calc_ix_encoded_size,
    handle_transaction_logs,
    log_message_for_node_to_fill,
    log_slots,
)
from perp_filler.src.node_utils import get_node_fill_info, get_user_account_from_map

from custom_log import get_custom_logger

logger = get_custom_logger(__name__)


async def execute_fillable_perp_nodes(perp_filler, nodes: list[NodeToFill]):
    logger.info(f"filling {len(nodes)} nodes")
    filled_node_count = 0
    while filled_node_count < len(nodes):
        attempted_fills = await try_bulk_fill_perp_nodes(
            perp_filler, nodes[filled_node_count:]
        )
        filled_node_count += attempted_fills


async def execute_triggerable_perp_nodes(perp_filler, nodes: list[NodeToTrigger]):
    logger.info(f"triggering {len(nodes)} nodes")
    for node in nodes:
        node.node.have_trigger = True
        user_account: Pubkey = node.node.user_account  # type: ignore
        order: Order = node.node.order  # type: ignore
        logger.info(
            f"trying to trigger: account: {str(user_account)} order: {order.order_id}"
        )
        user = await get_user_account_from_map(perp_filler, str(user_account))

        perp_filler.triggering_nodes[get_node_to_trigger_signature(node)] = time.time()

        ixs: list[Instruction] = []

        ix = perp_filler.drift_client.get_trigger_order_ix(user_account, user, order)

        ixs.append(ix)

        if perp_filler.revert_on_failure:
            ixs.append(perp_filler.drift_client.get_revert_fill_ix())

        sim_result = await simulate_and_get_tx_with_cus(
            ixs,
            perp_filler.drift_client,
            perp_filler.drift_client.tx_sender,
            perp_filler.lookup_tables,
            [],
            SIM_CU_ESTIMATE_MULTIPLIER,
            True,
            perp_filler.simulate_tx_for_cu_estimate,
        )

        logger.info(
            f"execute_triggerable_perp_nodes_for_market estimated CUs: {sim_result.cu_estimate}"
        )

        if perp_filler.simulate_tx_for_cu_estimate and sim_result.sim_error:
            logger.error(
                f"execute_triggerable_perp_nodes_for_market simerror: {sim_result.sim_error}"
            )
        else:
            try:
                tx_sig_and_slot: TxSigAndSlot = perp_filler.drift_client.tx_sender.send(
                    sim_result.tx
                )
                logger.info(
                    f"triggered node account: {str(user_account)} order: {order.order_id}"
                )
                logger.info(f"tx sig: {tx_sig_and_slot.tx_sig}")

            except Exception as e:
                node.node.have_trigger = False
                logger.error(
                    f"Failed to trigger node: {get_node_to_trigger_signature(node)}"
                )
                logger.error(f"Error: {e}")

            finally:
                perp_filler.remove_triggering_node(node)


async def try_bulk_fill_perp_nodes(perp_filler, nodes: list[NodeToFill]):
    micro_lamports = (
        min(
            math.floor(perp_filler.priority_fee_subscriber.max_priority_fee),
            MAX_CU_PRICE_MICRO_LAMPORTS,
        )
        or MAX_CU_PRICE_MICRO_LAMPORTS
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
    unique_accounts.add(str(perp_filler.drift_client.authority))

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
    fill_tx_id = perp_filler.fill_tx_id
    perp_filler.fill_tx_id += 1

    for idx, node in enumerate(nodes):
        if len(node.maker) > 1:
            # do multi maker fills in a separate tx since they're larger
            await try_fill_multi_maker_perp_nodes(perp_filler, node)
            nodes_sent.append(node)
            continue
        else:
            # We can't fill nodes against vAMM if the AMM is paused
            perp_market = perp_filler.drift_client.get_perp_market_account(node.node.order.market_index)  # type: ignore
            if is_variant(perp_market.status, "AmmPaused"):
                logger.error(f"Cannot fill node against vAMM on market: {node.node.order.market_index}, market status: {perp_market.status}")  # type: ignore
                continue
        logger.info(
            log_message_for_node_to_fill(
                node, f"Filling perp node {idx} (fill tx id: {fill_tx_id}) order slot: {node.node.order.slot}"  # type: ignore
            )
        )

        log_slots(perp_filler)

        node_fill_info = await get_node_fill_info(perp_filler, node)
        maker_infos = node_fill_info.maker_infos
        taker_user = node_fill_info.taker_user_account
        referrer_info = node_fill_info.referrer_info
        market_type = node_fill_info.market_type

        if not is_variant(market_type, "Perp"):
            raise ValueError("Expected perp market type")

        taker_pubkey = get_user_account_public_key(
            perp_filler.drift_client.program_id,
            taker_user.authority,
            taker_user.sub_account_id,
        )

        ix: Instruction = await perp_filler.drift_client.get_fill_perp_order_ix(
            taker_pubkey,
            taker_user,
            node.node.order,  # type: ignore
            maker_infos,
            referrer_info,
        )

        if not ix:
            logger.error("failed to generate an ix")
            break

        perp_filler.filling_nodes[get_node_to_fill_signature(node)] = time.time()

        # first estimate new tx size with additional ix and new accounts
        ix_keys = [account.pubkey for account in ix.accounts]
        new_accounts_unfiltered = ix_keys + [ix.program_id]
        new_accounts = [
            account
            for account in new_accounts_unfiltered
            if str(account) not in unique_accounts
        ]
        new_ix_cost = calc_ix_encoded_size(ix)
        additional_accounts_cost = (
            calc_compact_u16_encoded_size(new_accounts, 32) - 1
            if len(new_accounts) > 0
            else 0
        )

        # we have to use MAX_TX_PACK_SIZE because it appears we cannot send a tx with a size of exactly 1232
        # also might want to leave room for truncated logs near end of tx
        cu_to_use_per_fill = (
            BURST_CU_PER_FILL if perp_filler.use_burst_cu_limit else CU_PER_FILL
        )
        or_lhs = (
            running_tx_size + new_ix_cost + additional_accounts_cost >= MAX_TX_PACK_SIZE
        )
        or_rhs = running_cu_used + cu_to_use_per_fill >= MAX_CU_PER_TX
        and_rhs = len(ixs) > starting_size + 1  # ensure at least 1 attempted fill

        if (or_lhs or or_rhs) and and_rhs:
            logger.info(
                f"Fully packed fill tx (ixs: {len(ixs)}): est. tx size: "
                f"{running_tx_size + new_ix_cost + additional_accounts_cost}"
                f", max: {MAX_TX_PACK_SIZE}, est. CU used: expected: "
                f"{running_cu_used + cu_to_use_per_fill}, max: "
                f"{MAX_CU_PER_TX}, (fill tx id: {fill_tx_id})"
            )
            break

        logger.info(
            f"including taker: {str(taker_pubkey)}"
            f"-{node.node.order.order_id} (fill tx id: {fill_tx_id})"  # type: ignore
        )

        ixs.append(ix)
        running_tx_size += new_ix_cost + additional_accounts_cost
        running_cu_used += cu_to_use_per_fill

        for pubkey in new_accounts:
            unique_accounts.add(str(pubkey))
        idx_used += 1
        nodes_sent.append(node)

        if len(nodes_sent) == 0:
            return 0

        logger.info(
            f"sending tx: {len(unique_accounts)} unique accounts, "
            f"total ix: {idx_used}, calcd tx size: {running_tx_size} "
            f"took {time.time() - tx_packer_start}s (fill tx id: {fill_tx_id})"
        )

        if perp_filler.revert_on_failure:
            ixs.append(perp_filler.drift_client.get_revert_fill_ix())

        sim_result = await simulate_and_get_tx_with_cus(
            ixs,
            perp_filler.drift_client,
            perp_filler.drift_client.tx_sender,
            perp_filler.lookup_tables,
            [],
            SIM_CU_ESTIMATE_MULTIPLIER,
            True,
            perp_filler.simulate_tx_for_cu_estimate,
        )

        logger.info(
            f"try bulk fill estimate CUs: {sim_result.cu_estimate} (fill tx id: {fill_tx_id})"
        )

        if (
            int(sim_result.cu_estimate) < 50_000
            and sim_result.cu_estimate > 0
            and not sim_result.sim_error
        ):
            logger.warning("CU estimate very low (< 50,000)")
            logger.warning(f"CUs consumed: {sim_result.cu_estimate}")
            logger.warning("Possible error in transaction simulation")
            logger.warning("Simulation logs:")
            logger.warning(sim_result.sim_tx_logs)

        if perp_filler.simulate_tx_for_cu_estimate and sim_result.sim_error:
            logger.error(
                f"sim error: {str(sim_result.sim_error)} (fill tx id: {fill_tx_id})"
            )
            if sim_result.sim_tx_logs:
                await handle_transaction_logs(
                    perp_filler, [node], sim_result.sim_tx_logs
                )
                pass
        else:
            await send_fill_tx_and_parse_logs(perp_filler, fill_tx_id, sim_result.tx)
            pass

    return len(nodes_sent)


async def try_fill_multi_maker_perp_nodes(perp_filler, node: NodeToFill):
    micro_lamports = (
        min(
            math.floor(perp_filler.priority_fee_subscriber.max_priority_fee),
            MAX_CU_PRICE_MICRO_LAMPORTS,
        )
        or MAX_CU_PRICE_MICRO_LAMPORTS
    )
    ixs = [
        set_compute_unit_limit(MAX_CU_PER_TX),
        set_compute_unit_price(micro_lamports),
    ]

    fill_tx_id = perp_filler.fill_tx_id
    perp_filler.fill_tx_id += 1

    logger.info(
        log_message_for_node_to_fill(
            node,
            f"filling multi maker perp node with {len(node.maker)} makers (fill tx id: {fill_tx_id})",
        )
    )

    log_slots(perp_filler)

    try:
        node_fill_info = await get_node_fill_info(perp_filler, node)
        maker_infos = node_fill_info.maker_infos
        taker_user = node_fill_info.taker_user_account
        referrer_info = node_fill_info.referrer_info
        market_type = node_fill_info.market_type

        if not is_variant(market_type, "Perp"):
            raise ValueError("Expected perp market type")

        async def build_tx_with_maker_infos(makers: list[MakerInfo]):
            start = time.time()

            taker_pubkey = get_user_account_public_key(
                perp_filler.drift_client.program_id,
                taker_user.authority,
                taker_user.sub_account_id,
            )

            ix: Instruction = await perp_filler.drift_client.get_fill_perp_order_ix(
                taker_pubkey,
                taker_user,
                node.node.order,  # type: ignore
                makers,
                referrer_info,
            )

            ixs.append(ix)

            perp_filler.filling_nodes[get_node_to_fill_signature(node)] = time.time()

            if perp_filler.revert_on_failure:
                ixs.append(perp_filler.drift_client.get_revert_fill_ix())

            logger.info(
                f"build_tx_with_maker_infos took {time.time() - start}s (fill tx id: {fill_tx_id})"
            )

            return await simulate_and_get_tx_with_cus(
                ixs,
                perp_filler.drift_client,
                perp_filler.drift_client.tx_sender,
                perp_filler.lookup_tables,
                [],
                SIM_CU_ESTIMATE_MULTIPLIER,
                True,
                perp_filler.simulate_tx_for_cu_estimate,
            )

        sim_result = await build_tx_with_maker_infos(maker_infos)
        tx_accounts = len(sim_result.tx.message.account_keys)
        attempt = 0

        while tx_accounts > MAX_ACCOUNTS_PER_TX and maker_infos > 0:
            logger.info(
                f"(fill tx id: {fill_tx_id} attempt: {attempt}) too many accounts, removing 1 and trying again "
                f"has {len(maker_infos)} makers and {tx_accounts} accounts"
            )
            maker_infos = maker_infos[:-1]
            sim_result = await build_tx_with_maker_infos(maker_infos)
            tx_accounts = len(sim_result.tx.message.account_keys)

        if len(maker_infos) == 0:
            logger.error(
                f"No makerinfos left to use for multi maker node (fill tx id: {fill_tx_id})"
            )
            return

        logger.info(
            f"try_fill_multi_maker_perp_nodes estimated CUs: {sim_result.cu_estimate} (fill tx id: {fill_tx_id})"
        )

        if perp_filler.simulate_tx_for_cu_estimate and sim_result.sim_error:
            logger.error(
                f"sim error: {str(sim_result.sim_error)} (fill tx id: {fill_tx_id})"
            )
            if sim_result.sim_tx_logs:
                await handle_transaction_logs(
                    perp_filler, [node], sim_result.sim_tx_logs
                )
                pass
        else:
            await send_fill_tx_and_parse_logs(perp_filler, fill_tx_id, sim_result.tx)
            pass

    except Exception as e:
        logger.error(f"Error filling multi maker perp node (fill tx id: {fill_tx_id})")
        logger.error(e)


async def send_fill_tx_and_parse_logs(
    perp_filler, fill_tx_id: int, tx: VersionedTransaction
):
    start = time.time()
    try:
        tx_resp: TxSigAndSlot = await perp_filler.drift_client.tx_sender.send(tx)
        logger.success(f"sent fill tx: {tx_resp.tx_sig} (fill tx id: {fill_tx_id})")
        logger.success(f"took: {time.time() - start}s")
    except Exception as e:
        if "RevertFill" in str(e):
            logger.error(f"Fill reverted (fill tx id: {fill_tx_id})")
        else:
            logger.critical(f"Failed to send fill transaction: {e}")
