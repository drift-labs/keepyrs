import math
import time
import logging

from typing import Optional, Union
from dataclasses import dataclass

from solana.rpc.types import TxOpts
from solana.rpc.core import _COMMITMENT_TO_SOLDERS

from solana.transaction import Transaction

from solders.transaction import VersionedTransaction
from solders.transaction_status import TransactionErrorType
from solders.instruction import Instruction
from solders.address_lookup_table_account import AddressLookupTableAccount
from solders.pubkey import Pubkey
from solders.keypair import Keypair
from solders.compute_budget import set_compute_unit_limit
from solders.rpc.config import RpcSimulateTransactionConfig
from solders.rpc.requests import SimulateVersionedTransaction
from solders.rpc.responses import SimulateTransactionResp

from driftpy.drift_client import DriftClient
from driftpy.tx.standard_tx_sender import StandardTxSender

from driftpy.dlob.dlob import DLOB, NodeToFill, NodeToTrigger
from driftpy.dlob.dlob_node import DLOBNode
from driftpy.dlob.node_list import get_order_signature
from driftpy.types import OraclePriceData, MarketType

COMPUTE_BUDGET_PROGRAM = Pubkey.from_string(
    "ComputeBudget111111111111111111111111111111"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SimulateAndGetTxWithCUsResponse:
    cu_estimate: int
    tx: VersionedTransaction
    sim_tx_logs: Optional[list[str]] = None
    sim_error: Optional[Union[TransactionErrorType, str]] = None


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


def is_set_compute_units_ix(ix: Instruction) -> bool:
    if (ix.program_id == COMPUTE_BUDGET_PROGRAM) and (ix.data[0] == 2):
        return True
    return False


async def simulate_and_get_tx_with_cus(
    ixs: list[Instruction],
    drift_client: DriftClient,
    tx_sender: StandardTxSender,
    lookup_tables: AddressLookupTableAccount,
    additional_signers: list[Keypair],
    opts: Optional[TxOpts] = None,
    cu_limit_multiplier: float = 1.0,
    log_sim_duration: bool = False,
    do_sim: bool = True,
):
    str_err: Optional[str] = None

    if len(ixs) == 0:
        raise ValueError("cannot simulate empty tx")

    set_cu_limit_ix_idx = -1
    for idx, ix in enumerate(ixs):
        if is_set_compute_units_ix(ix):
            set_cu_limit_ix_idx = idx
            break

    tx = await tx_sender.get_versioned_tx(
        ixs, drift_client.wallet.payer, lookup_tables, additional_signers  # type: ignore
    )

    if not do_sim:
        return SimulateAndGetTxWithCUsResponse(-1, tx)

    try:
        start = time.time()

        # manually simulate transaction because we need replace recent blockhash, which AsyncClient.simulate_transaction() doesn't expose
        commitment = _COMMITMENT_TO_SOLDERS[drift_client.connection.commitment]
        config = RpcSimulateTransactionConfig(
            commitment=commitment, replace_recent_blockhash=True
        )
        body = SimulateVersionedTransaction(tx, config)

        resp = await drift_client.connection._provider.make_request(
            body, SimulateTransactionResp
        )

        if log_sim_duration:
            logger.info(f"Simulated tx took: {time.time() - start} time")

    except Exception as e:
        logger.error(e)

    if not resp:
        raise ValueError("Failed to simulate transaction")

    if resp.value.units_consumed is None:
        raise ValueError("Failed to get CUs from simulate transaction")

    sim_tx_logs = resp.value.logs

    order_not_exist = any("Order does not exist" in log for log in sim_tx_logs)

    if order_not_exist:
        logger.error("Order already filled")
        str_err = "Order already filled"

    cu_estimate = resp.value.units_consumed

    if set_cu_limit_ix_idx == -1:
        ixs.insert(0, set_compute_unit_limit(int(cu_estimate * cu_limit_multiplier)))
    else:
        ixs[set_cu_limit_ix_idx] = set_compute_unit_limit(
            int(cu_estimate * cu_limit_multiplier)
        )

    tx = await tx_sender.get_versioned_tx(
        ixs, drift_client.wallet.payer, lookup_tables, additional_signers  # type: ignore
    )

    return SimulateAndGetTxWithCUsResponse(
        cu_estimate, tx, sim_tx_logs, resp.value.err or str_err
    )
