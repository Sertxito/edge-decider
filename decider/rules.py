def light_bucket(analog, bright_max=200, ambient_max=450):
    if analog is None:
        return "unknown"
    try:
        v = float(analog)
    except:
        return "unknown"
    if v < bright_max:
        return "bright"
    if v < ambient_max:
        return "ambient"
    return "dark"

def temp_band(temp_c, low=15.0, high=30.0):
    if temp_c is None:
        return "unknown"
    try:
        v = float(temp_c)
    except:
        return "unknown"
    if v < low:
        return "low"
    if v > high:
        return "high"
    return "normal"

def hum_band(humidity, low=20.0, high=80.0):
    if humidity is None:
        return "unknown"
    try:
        v = float(humidity)
    except:
        return "unknown"
    if v < low:
        return "low"
    if v > high:
        return "high"
    return "normal"

def is_intrusion(door_state, motion):
    return (door_state == "OPEN") and bool(motion)
