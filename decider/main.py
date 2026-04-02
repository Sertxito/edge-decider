import json
import os
import time
import logging
import threading
from azure.iot.device import IoTHubModuleClient, Message

from decider.events import alarm_event, intrusion_event, arm_event, aggregate_event, heartbeat_event
from decider.rules import calc_intrusion

INPUT_NAME = os.getenv("EDGEDECIDER_INPUT", "input1")
OUTPUT_NAME = os.getenv("EDGEDECIDER_OUTPUT", "output1")

AGGREGATE_WINDOW_SEC = int(os.getenv("EDGEDECIDER_AGG_WINDOW_SEC", "60"))
HEARTBEAT_INTERVAL_SEC = int(os.getenv("EDGEDECIDER_HEARTBEAT_SEC", "300"))
ALARM_COOLDOWN_SEC = int(os.getenv("EDGEDECIDER_ALARM_COOLDOWN_SEC", "60"))

LOG_LEVEL = os.getenv("EDGEDECIDER_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(levelname)s] %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("edgeDecider")

def make_msg(payload: dict) -> Message:
    m = Message(json.dumps(payload))
    m.content_type = "application/json"
    m.content_encoding = "utf-8"
    return m

# ---------- shared state ----------
lock = threading.Lock()
latest = None
latest_device_id = "unknown"
latest_received_ts = 0.0
seq = 0

# aggregates
buf_light = []
last_temp = None
last_hum = None

# alarms + cooldown
alarm_active = {"gas": False, "pir": False}
last_alarm_emit = {"gas": 0.0, "pir": 0.0}

# arm state
armed = False
last_touch = False

def _is_dict(x): return isinstance(x, dict)

def normalize(data_in: dict) -> dict:
    """
    Accepts:
      - sensorSim-like (dict sensors)
      - NodeMCU raw (bool/str sensors)
    Returns a unified dict with safe accessors.
    """
    device_id = data_in.get("deviceId", "unknown")
    sensors = data_in.get("sensors", {})
    if not isinstance(sensors, dict):
        sensors = {}

    # If it already looks like sensorSim (gas/light/pir are dicts) keep it but
    # also extract door/touch if present in raw keys.
    looks_sensorsim = any(_is_dict(sensors.get(k)) for k in ("gas","light","pir","dht11","sound"))
    if looks_sensorsim:
        # enrich optional fields
        door = sensors.get("door")
        touch = sensors.get("touch")
        if isinstance(door, str) or isinstance(touch, bool):
            data_in["_meta"] = {"door": door, "touch": touch}
        return data_in

    # NodeMCU raw mapping
    motion = bool(sensors.get("motion", False))
    door = sensors.get("door", "UNKNOWN")
    if not isinstance(door, str):
        door = "UNKNOWN"
    gas_bool = bool(sensors.get("gas", False))
    touch_bool = bool(sensors.get("touch", False))
    ldr_raw = sensors.get("ldr_raw", None)
    temp = sensors.get("temperature", None)
    hum = sensors.get("humidity", None)

    out = {
        "deviceId": device_id,
        "sensors": {
            "gas": {"analog": None, "alarm": gas_bool},
            "sound": {"analog": None, "alarm": False},
            "light": {"analog": ldr_raw if isinstance(ldr_raw, (int,float)) else None},
            "pir": {"motion": motion},
            "dht11": {
                "temp_c": temp if isinstance(temp, (int,float)) else None,
                "humidity": hum if isinstance(hum, (int,float)) else None
            }
        },
        "_meta": {"door": door, "touch": touch_bool}
    }
    return out

def on_message_received(message):
    global latest, latest_device_id, latest_received_ts, seq
    global buf_light, last_temp, last_hum
    try:
        in_name = getattr(message, "input_name", None)
        if in_name is not None and in_name != INPUT_NAME:
            return

        raw = message.data.decode("utf-8")
        log.info(f"[INPUT] Received on {INPUT_NAME}: {raw}")

        data_in = json.loads(raw)
        if not isinstance(data_in, dict):
            raise ValueError("JSON is not an object")

        data = normalize(data_in)
        device_id = data.get("deviceId", "unknown")
        sensors = data.get("sensors", {}) if isinstance(data.get("sensors", {}), dict) else {}

        # safe dicts
        light = sensors.get("light") if _is_dict(sensors.get("light")) else {}
        dht = sensors.get("dht11") if _is_dict(sensors.get("dht11")) else {}

        with lock:
            seq += 1
            latest = data
            latest_device_id = device_id
            latest_received_ts = time.time()

            la = light.get("analog", None)
            if isinstance(la, (int,float)):
                buf_light.append(la)

            t = dht.get("temp_c", None)
            h = dht.get("humidity", None)
            if isinstance(t, (int,float)):
                last_temp = t
            if isinstance(h, (int,float)):
                last_hum = h

    except Exception as e:
        log.error(f"Failed to parse input message: {e}")

def avg(values):
    return (sum(values) / len(values)) if values else None

def main():
    global buf_light, last_temp, last_hum
    global armed, last_touch

    client = IoTHubModuleClient.create_from_edge_environment()
    client.on_message_received = on_message_received
    client.connect()

    start_ts = time.time()
    last_agg_ts = time.time()
    last_hb_ts = time.time()

    log.info("edgeDecider started")
    log.info(f"[DECIDER] Listening on {INPUT_NAME}...")

    while True:
        now = time.time()

        with lock:
            data = latest
            device_id = latest_device_id
            last_seen = latest_received_ts
            seq_value = seq

        # --- rules/events ---
        if data:
            sensors = data.get("sensors", {}) if isinstance(data.get("sensors", {}), dict) else {}
            gas = sensors.get("gas") if _is_dict(sensors.get("gas")) else {}
            pir = sensors.get("pir") if _is_dict(sensors.get("pir")) else {}

            # meta for door/touch (from NodeMCU)
            meta = data.get("_meta", {}) if isinstance(data.get("_meta", {}), dict) else {}
            door = meta.get("door", "UNKNOWN")
            touch = bool(meta.get("touch", False))

            # ---- ARM toggle on touch rising edge ----
            if touch and not last_touch:
                armed = not armed
                client.send_message_to_output(make_msg(arm_event(device_id, armed)), OUTPUT_NAME)
                log.info(f"ARM toggled -> {armed}")
            last_touch = touch

            # ---- Gas alarm edge-triggered + cooldown ----
            gas_alarm = bool(gas.get("alarm", False))
            prev_gas = alarm_active["gas"]
            if gas_alarm and not prev_gas:
                alarm_active["gas"] = True
                last_alarm_emit["gas"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "gas", gas, "raised")), OUTPUT_NAME)
                log.info("Alarm RAISED: gas")
            elif gas_alarm and prev_gas and (now - last_alarm_emit["gas"] >= ALARM_COOLDOWN_SEC):
                last_alarm_emit["gas"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "gas", gas, "reminder")), OUTPUT_NAME)
                log.info("Alarm REMINDER: gas")
            elif (not gas_alarm) and prev_gas:
                alarm_active["gas"] = False
                last_alarm_emit["gas"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "gas", gas, "cleared", severity="info")), OUTPUT_NAME)
                log.info("Alarm CLEARED: gas")

            # ---- Intrusion: only if armed ----
            motion = bool(pir.get("motion", False))
            if armed and calc_intrusion(door, motion):
                ctx = {"door": door, "motion": motion}
                client.send_message_to_output(make_msg(intrusion_event(device_id, ctx)), OUTPUT_NAME)
                log.info("Event: INTRUSION")

        # --- Aggregate ---
        if now - last_agg_ts >= AGGREGATE_WINDOW_SEC:
            with lock:
                light_avg = avg(buf_light)
                t = last_temp
                h = last_hum
                buf_light = []
            client.send_message_to_output(make_msg(aggregate_event(device_id, AGGREGATE_WINDOW_SEC, light_avg, t, h)), OUTPUT_NAME)
            log.info("Aggregate sent")
            last_agg_ts = now

        # --- Heartbeat ---
        if now - last_hb_ts >= HEARTBEAT_INTERVAL_SEC:
            uptime_sec = int(now - start_ts)
            last_age = int(now - last_seen) if last_seen > 0 else None
            client.send_message_to_output(make_msg(heartbeat_event(device_id, uptime_sec, last_age, seq_value)), OUTPUT_NAME)
            log.info("Heartbeat sent")
            last_hb_ts = now

        time.sleep(0.2)

if __name__ == "__main__":
    main()
