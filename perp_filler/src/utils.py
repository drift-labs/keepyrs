from solders.instruction import Instruction

from perp_filler.src.perp_filler import PerpFiller


def get_latest_slot(perp_filler: PerpFiller) -> int:
    return max(perp_filler.slot_subscriber.get_slot(), perp_filler.user_map.get_slot())


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
