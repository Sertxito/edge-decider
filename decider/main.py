import json
import os
import time
import logging
import threading
from azure.iot.device import IoTHubModuleClient, Message

from decider.events import alarm_event, intrusion_event, arm_event, aggregate_event, heartbeat_event
from decider.rules import calc_intrusion

INPUT_NAME = os.getenv("ED_IN", os.getenv("EDGEDECIDER_INPUT", "input1"))
OUTPUT_NAME = os.getenv("ED_OUT", os.getenv("EDGEDECIDER_OUTPUT", "output1"))

AGGREGATE_WINDOW_SEC = int(os.getenv("AGG_WIN_SEC", os.getenv("EDGEDECIDER_AGG_WINDOW_SEC", "60")))
HEARTBEAT_INTERVAL_SEC = int(os.getenv("HB_SEC", os.getenv("EDGEDECIDER_HEARTBEAT_SEC", "300")))
ALARM_COOLDOWN_SEC = int(os.getenv("ALARM_CD_SEC", os.getenv("EDGEDECIDER_ALARM_COOLDOWN_SEC", "60")))

# ✅ Política B: publicar aggregates al cloud cada X segundos (por defecto 15 min)
AGGREGATE_PUBLISH_INTERVAL_SEC = int(os.getenv("AGG_PUB_SEC", os.getenv("EDGEDECIDER_AGG_PUBLISH_SEC", "900")))

LOG_LEVEL = os.getenv("LOG", os.getenv("EDGEDECIDER_LOG_LEVEL", "INFO")).upper()

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

lock = threading.Lock()
latest = None
latest_device_id = "unknown"
latest_received_ts = 0.0
seq = 0

buf_light = []
last_temp = None
last_hum = None

alarm_active = {"gas": False, "pir": False}
last_alarm_emit = {"gas": 0.0, "pir": 0.0}

armed = False
last_touch = False

def _is_dict(x): return isinstance(x, dict)

def normalize(data_in: dict) -> dict:
    device_id = data_in.get("deviceId", "unknown")
    sensors = data_in.get("sensors", {})
    if not isinstance(sensors, dict):
        sensors = {}

    looks_sensorsim = any(_is_dict(sensors.get(k)) for k in ("gas","light","pir","dht11","sound"))
    if looks_sensorsim:
        door = sensors.get("door")
        touch = sensors.get("touch")
        if isinstance(door, str) or isinstance(touch, bool):
            data_in["_meta"] = {"door": door, "touch": touch}
        return data_in

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
    last_agg_publish_ts = 0.0  # ✅ control de publicación cloud (política B)

    log.info("edgeDecider started")
    log.info(f"[DECIDER] Listening on {INPUT_NAME}...")
    log.info(f"[DECIDER] Aggregate publish interval: {AGGREGATE_PUBLISH_INTERVAL_SEC}s")

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

            meta = data.get("_meta", {}) if isinstance(data.get("_meta", {}), dict) else {}
            door = meta.get("door", "UNKNOWN")
            touch = bool(meta.get("touch", False))

            # ARM toggle on touch rising edge
            if touch and not last_touch:
                armed = not armed
                client.send_message_to_output(make_msg(arm_event(device_id, armed)), OUTPUT_NAME)
                log.info(f"ARM toggled -> {armed}")
            last_touch = touch

            # Gas alarm edge-triggered + cooldown
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

            # PIR alarm
            motion = bool(pir.get("motion", False))
            prev_pir = alarm_active["pir"]
            if motion and not prev_pir:
                alarm_active["pir"] = True
                last_alarm_emit["pir"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "pir", pir, "raised")), OUTPUT_NAME)
                log.info("Alarm RAISED: pir")
            elif (not motion) and prev_pir:
                alarm_active["pir"] = False
                last_alarm_emit["pir"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "pir", pir, "cleared", severity="info")), OUTPUT_NAME)
                log.info("Alarm CLEARED: pir")

            # Intrusion: only if armed
            if armed and calc_intrusion(door, motion):
                ctx = {"door": door, "motion": motion}
                client.send_message_to_output(make_msg(intrusion_event(device_id, ctx)), OUTPUT_NAME)
                log.info("Event: INTRUSION")

        # --- Aggregate (compute always, publish periodically) ---
        if now - last_agg_ts >= AGGREGATE_WINDOW_SEC:
            with lock:
                light_avg = avg(buf_light)
                t = last_temp
                h = last_hum
                buf_light = []

            evt = aggregate_event(device_id, AGGREGATE_WINDOW_SEC, light_avg, t, h)

            if now - last_agg_publish_ts >= AGGREGATE_PUBLISH_INTERVAL_SEC:
                client.send_message_to_output(make_msg(evt), OUTPUT_NAME)
                last_agg_publish_ts = now
                log.info("Aggregate PUBLISHED to cloud")
            else:
                log.info("Aggregate computed (kept in edge)")

            last_agg_ts = now

        # --- Heartbeat (always) ---
        if now - last_hb_ts >= HEARTBEAT_INTERVAL_SEC:
            uptime_sec = int(now - start_ts)
            last_age = int(now - last_seen) if last_seen > 0 else None
            client.send_message_to_output(make_msg(heartbeat_event(device_id, uptime_sec, last_age, seq_value)), OUTPUT_NAME)
            log.info("Heartbeat sent")
            last_hb_ts = now

        time.sleep(0.2)

if __name__ == "__main__":
    main()
