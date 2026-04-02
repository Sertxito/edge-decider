def calc_intrusion(door_state: str, motion: bool) -> bool:
    return (door_state == "OPEN") and bool(motion)

def calc_gas_alarm(gas_alarm: bool) -> bool:
    return bool(gas_alarm)
