from datetime import datetime, timezone

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def event_base(event_type, device_id, severity="info", data=None, extra=None):
    payload = {
        "type": event_type,
        "deviceId": device_id,
        "ts": utc_now(),
        "source": "edgeDecider",
        "severity": severity,
        "data": data or {}
    }
    if extra:
        payload.update(extra)
    return payload

def alarm_event(device_id, sensor_name, raw, state, severity="high"):
    return event_base(
        "alarm",
        device_id,
        severity=severity,
        data={"sensor": sensor_name, "raw": raw, "state": state}
    )

def state_event(device_id, name, state, value=None, armed=None, extra=None):
    data = {"name": name, "state": state}
    if value is not None:
        data["value"] = value
    if armed is not None:
        data["armed"] = bool(armed)
    if extra:
        data.update(extra)
    return event_base("state.change", device_id, severity="info", data=data)

def intrusion_event(device_id, door_state, motion, armed):
    return event_base(
        "security.intrusion",
        device_id,
        severity="high",
        data={"door": door_state, "motion": bool(motion), "armed": bool(armed)}
    )

def aggregate_event(device_id, window_sec, system, sensors):
    return {
        "type": "aggregate",
        "deviceId": device_id,
        "ts": utc_now(),
        "source": "edgeDecider",
        "window_sec": int(window_sec),
        "system": system,
        "sensors": sensors
    }
