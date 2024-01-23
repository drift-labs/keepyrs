import random

from typing import Union

from driftpy.math.conversion import convert_to_number
from driftpy.dlob.dlob_node import DLOBNode
from driftpy.math.utils import div_ceil

from keepyr_types import MakerNodeMap
from perp_filler.src.constants import MAX_MAKERS_PER_FILL

PROBABILITY_PRECISION = 1_000


def select_makers(maker_node_map: MakerNodeMap) -> MakerNodeMap:
    selected_makers: MakerNodeMap = {}

    while len(selected_makers) < MAX_MAKERS_PER_FILL and maker_node_map:
        maker = select_maker(maker_node_map)
        if maker is None:
            break
        maker_nodes = maker_node_map.pop(maker, None)
        if maker_nodes is not None:
            selected_makers[maker] = maker_nodes

    return selected_makers


def select_maker(maker_node_map: MakerNodeMap) -> Union[str, None]:
    if len(maker_node_map) == 0:
        return None

    total_liquidity = 0
    for nodes in maker_node_map.values():
        total_liquidity += get_maker_liquidity(nodes)

    probabilities: list[int] = []
    for nodes in maker_node_map.values():
        probabilities.append(get_probability(nodes, total_liquidity))

    maker_index = 0
    rnd = random.random()
    sum = 0
    for i, probability in enumerate(probabilities):
        sum += probability
        if rnd < sum:
            maker_index = i
            break

    return list(maker_node_map.keys())[maker_index]


def get_probability(dlob_nodes: list[DLOBNode], total_liquidity: int) -> int:
    maker_liquidity = get_maker_liquidity(dlob_nodes)
    return convert_to_number(
        div_ceil(maker_liquidity * PROBABILITY_PRECISION, total_liquidity),
        PROBABILITY_PRECISION,
    )


def get_maker_liquidity(dlob_nodes: list[DLOBNode]) -> int:
    total_liquidity = 0
    for dlob_node in dlob_nodes:
        total_liquidity += dlob_node.order.base_asset_amount - dlob_node.order.base_asset_amount_filled  # type: ignore
    return total_liquidity
