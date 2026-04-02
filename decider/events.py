import json
from datetime import datetime, timezone

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def alarm_event(device_id, sensor_name, raw, state, severity="high"):
    return {
        "type": "alarm",
        "state": state,  # raised | cleared | reminder
        "deviceId": device_id,
        "ts": utc_now(),
        "source": "edgeDecider",
        "severity": severity,
        "data": {"sensor": sensor_name, "raw": raw}
    }

def intrusion_event(device_id, context, severity="high"):
    return {
        "type": "security.intrusion",
        "deviceId": device_id,
        "ts": utc_now(),
        "source": "edgeDecider",
        "severity": severity,
        "data": context
    }

def arm_event(device_id, armed: bool):
    return {
        "type": "control.arm_state",
        "deviceId": device_id,
        "ts": utc_now(),
        "source": "edgeDecider",
        "status": "ARMED" if armed else "DISARMED"
    }

def aggregate_event(device_id, window_sec, light_avg, temp_c, humidity):
    return {
        "type": "aggregate",
        "deviceId": device_id,
        "ts": utc_now(),
        "source": "edgeDecider",
        "window_sec": window_sec,
        "data": {
            "light_avg": light_avg,
            "temp_c": temp_c,
            "humidity": humidity
        }
    }

def heartbeat_event(device_id, uptime_sec, last_input_age_sec, seq):
    return {
        "type": "heartbeat",
        "deviceId": device_id,
        "ts": utc_now(),
        "source": "edgeDecider",
        "status": "ok",
        "uptime_sec": uptime_sec,
        "last_input_age_sec": last_input_age_sec,
        "seq": seq
    }
