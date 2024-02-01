import re


def is_end_ix_log(program_id: str, log: str) -> bool:
    pattern = f"Program {program_id} consumed ([0-9]+) of ([0-9]+) compute units"
    match = re.search(pattern, log)
    return bool(match)


def is_ix_log(log: str) -> bool:
    return bool(re.search("Program log: Instruction:", log))


def is_fill_ix_log(log: str) -> bool:
    return bool(re.search("Program log: Instruction: Fill.*Order", log))


def is_order_does_not_exist_log(log: str):
    match = re.search(".*Order does not exist ([0-9]+)", log)
    return int(match.group(1)) if match else None


def is_maker_breached_maintenance_margin_log(log: str):
    match = re.search(
        ".*maker \(([1-9A-HJ-NP-Za-km-z]+)\) breached (maintenance|fill) requirements.*$",
        log,
    )
    return match.group(1) if match else None


def is_taker_breached_maintenance_margin_log(log: str) -> bool:
    return bool(re.search(".*taker breached (maintenance|fill) requirements.*", log))


def is_err_filling_log(log: str):
    match = re.search(".*Err filling order id ([0-9]+) for user ([a-zA-Z0-9]+)", log)
    return (match.group(1), match.group(2)) if match else None


def is_err_stale_oracle(log: str) -> bool:
    return bool(re.search(".*Invalid Oracle: Stale.*", log))
