import json
import os
import time
import logging
import threading
from azure.iot.device import IoTHubModuleClient, Message

from decider.events import alarm_event, state_event, intrusion_event, aggregate_event
from decider.rules import light_bucket, temp_band, hum_band, is_intrusion

# ---- Env (cortas + fallback largas) ----
INPUT_NAME  = os.getenv("ED_IN",  os.getenv("EDGEDECIDER_INPUT",  "input1"))
OUTPUT_NAME = os.getenv("ED_OUT", os.getenv("EDGEDECIDER_OUTPUT", "output1"))

# Ventana de muestreo interna (cada cuánto calculamos bucket/estados)
SAMPLE_TICK_SEC = float(os.getenv("SAMPLE_TICK_SEC", "0.2"))

# Aggregate: cada cuánto se publica (foto de N segundos)
AGG_PUB_SEC = int(os.getenv("AGG_PUB_SEC", os.getenv("EDGEDECIDER_AGG_PUBLISH_SEC", "900")))

# Alarm cooldown (recordatorio si gas sigue activo)
ALARM_CD_SEC = int(os.getenv("ALARM_CD_SEC", os.getenv("EDGEDECIDER_ALARM_COOLDOWN_SEC", "60")))

# Thresholds
LIGHT_BRIGHT_MAX  = int(os.getenv("LIGHT_BRIGHT_MAX",  "200"))
LIGHT_AMBIENT_MAX = int(os.getenv("LIGHT_AMBIENT_MAX", "450"))
TEMP_LOW  = float(os.getenv("TEMP_LOW",  "15"))
TEMP_HIGH = float(os.getenv("TEMP_HIGH", "30"))
HUM_LOW   = float(os.getenv("HUM_LOW",   "20"))
HUM_HIGH  = float(os.getenv("HUM_HIGH",  "80"))

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

# ---- shared state ----
lock = threading.Lock()
latest = None
latest_device_id = "unknown"
latest_received_ts = 0.0
seq = 0

# buffers para aggregate de N minutos
buf_light = []
buf_temp  = []
buf_hum   = []

# estados actuales
state = {
    "armed": False,
    "door": "UNKNOWN",
    "motion": False,
    "gas_alarm": False,
    "light_bucket": "unknown",
    "temp_band": "unknown",
    "hum_band": "unknown",
    "temp_c": None,
    "hum": None,
    "light_analog": None
}

# últimos estados para emitir eventos por cambio (state.change)
last_emitted = {
    "armed": None,
    "door": None,
    "motion": None,
    "light_bucket": None,
    "temp_band": None,
    "hum_band": None,
    "gas_alarm": None
}

# alarm edge-trigger + cooldown
alarm_active = {"gas": False, "pir": False}
last_alarm_emit = {"gas": 0.0, "pir": 0.0}

# intrusion edge-trigger
intrusion_active = False

def _is_dict(x): return isinstance(x, dict)

def normalize(data_in: dict) -> dict:
    """
    Acepta:
      - sensorSim-like: sensors.gas/light/pir/dht11 como dict
      - NodeMCU raw: gas(bool), motion(bool), ldr_raw(int), temperature(float), humidity(float), door(str), touch(bool)
    Debe conservar _meta si viene (door/touch).
    """
    device_id = data_in.get("deviceId", "unknown")
    sensors = data_in.get("sensors", {})
    if not isinstance(sensors, dict):
        sensors = {}

    looks_sensorsim = any(_is_dict(sensors.get(k)) for k in ("gas","light","pir","dht11","sound"))
    if looks_sensorsim:
        # mantener _meta si viene
        meta = data_in.get("_meta")
        if isinstance(meta, dict):
            data_in["_meta"] = meta
        return data_in

    # NodeMCU raw -> sensorSim-like + meta
    motion = bool(sensors.get("motion", False))
    gas_bool = bool(sensors.get("gas", False))
    ldr_raw = sensors.get("ldr_raw", None)
    temp = sensors.get("temperature", None)
    hum = sensors.get("humidity", None)

    door = sensors.get("door", "UNKNOWN")
    if not isinstance(door, str):
        door = "UNKNOWN"
    touch = bool(sensors.get("touch", False))

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
        "_meta": {"door": door, "touch": touch}
    }
    return out

def on_message_received(message):
    global latest, latest_device_id, latest_received_ts, seq
    global buf_light, buf_temp, buf_hum

    try:
        in_name = getattr(message, "input_name", None)
        if in_name is not None and in_name != INPUT_NAME:
            return

        raw = message.data.decode("utf-8")
        data_in = json.loads(raw)
        if not isinstance(data_in, dict):
            return

        data = normalize(data_in)
        device_id = data.get("deviceId", "unknown")

        sensors = data.get("sensors", {}) if isinstance(data.get("sensors"), dict) else {}
        meta = data.get("_meta", {}) if isinstance(data.get("_meta"), dict) else {}

        light = sensors.get("light") if _is_dict(sensors.get("light")) else {}
        dht   = sensors.get("dht11") if _is_dict(sensors.get("dht11")) else {}

        la = light.get("analog")
        t  = dht.get("temp_c")
        h  = dht.get("humidity")

        with lock:
            seq += 1
            latest = data
            latest_device_id = device_id
            latest_received_ts = time.time()

            # buffers de 15 min (acumulan hasta publicar)
            if isinstance(la, (int,float)): buf_light.append(float(la))
            if isinstance(t,  (int,float)): buf_temp.append(float(t))
            if isinstance(h,  (int,float)): buf_hum.append(float(h))

    except Exception as e:
        log.error(f"Failed to parse input message: {e}")

def avg(values):
    return (sum(values) / len(values)) if values else None

def emit_if_changed(client, device_id, name, new_value, value_for_payload=None, armed_flag=None, extra=None):
    if last_emitted.get(name) != new_value:
        last_emitted[name] = new_value
        client.send_message_to_output(
            make_msg(state_event(device_id, name, new_value, value=value_for_payload, armed=armed_flag, extra=extra)),
            OUTPUT_NAME
        )
        log.info(f"State change emitted: {name} -> {new_value}")

def main():
    global intrusion_active

    client = IoTHubModuleClient.create_from_edge_environment()
    client.on_message_received = on_message_received
    client.connect()

    start_ts = time.time()
    last_agg_pub_ts = time.time()

    log.info("edgeDecider started")
    log.info(f"[DECIDER] Listening on {INPUT_NAME}...")
    log.info(f"[DECIDER] Aggregate publish interval: {AGG_PUB_SEC}s")

    while True:
        now = time.time()

        with lock:
            data = latest
            device_id = latest_device_id
            last_seen = latest_received_ts
            seq_value = seq

        if data:
            sensors = data.get("sensors", {}) if isinstance(data.get("sensors"), dict) else {}
            meta = data.get("_meta", {}) if isinstance(data.get("_meta"), dict) else {}

            gas = sensors.get("gas") if _is_dict(sensors.get("gas")) else {}
            pir = sensors.get("pir") if _is_dict(sensors.get("pir")) else {}
            light = sensors.get("light") if _is_dict(sensors.get("light")) else {}
            dht = sensors.get("dht11") if _is_dict(sensors.get("dht11")) else {}

            # actuales
            gas_alarm = bool(gas.get("alarm", False))
            motion = bool(pir.get("motion", False))
            door = meta.get("door", "UNKNOWN")
            if not isinstance(door, str):
                door = "UNKNOWN"
            touch = bool(meta.get("touch", False))

            la = light.get("analog", None)
            t = dht.get("temp_c", None)
            h = dht.get("humidity", None)

            # --- armado: 1 = armado, 0 = desarmado (persistencia en runtime) ---
            new_armed = True if touch else False
            if new_armed != state["armed"]:
                state["armed"] = new_armed
                emit_if_changed(client, device_id, "armed", "ARMED" if new_armed else "DISARMED")

            # --- door open/close event (si tenemos dato) ---
            if door != state["door"]:
                state["door"] = door
                emit_if_changed(client, device_id, "door", "open" if door == "OPEN" else "closed",
                                armed_flag=state["armed"], extra={"door_raw": door})

            # --- motion on/off event (no-alarm) ---
            if motion != state["motion"]:
                state["motion"] = motion
                emit_if_changed(client, device_id, "motion", "active" if motion else "inactive",
                                armed_flag=state["armed"])

            # --- light bucket change event ---
            lb = light_bucket(la, bright_max=LIGHT_BRIGHT_MAX, ambient_max=LIGHT_AMBIENT_MAX)
            state["light_bucket"] = lb
            state["light_analog"] = la
            emit_if_changed(client, device_id, "light_bucket", lb, value_for_payload=la)

            # --- temperature band change event ---
            tb = temp_band(t, low=TEMP_LOW, high=TEMP_HIGH)
            state["temp_band"] = tb
            state["temp_c"] = t
            emit_if_changed(client, device_id, "temp_band", tb, value_for_payload=t)

            # --- humidity band change event ---
            hb = hum_band(h, low=HUM_LOW, high=HUM_HIGH)
            state["hum_band"] = hb
            state["hum"] = h
            emit_if_changed(client, device_id, "hum_band", hb, value_for_payload=h)

            # --- GAS alarm edge-triggered + cooldown (tipo alarm) ---
            prev_gas = alarm_active["gas"]
            if gas_alarm and not prev_gas:
                alarm_active["gas"] = True
                last_alarm_emit["gas"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "gas", gas, "raised")), OUTPUT_NAME)
                log.info("Alarm RAISED: gas")
            elif gas_alarm and prev_gas and (now - last_alarm_emit["gas"] >= ALARM_CD_SEC):
                last_alarm_emit["gas"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "gas", gas, "reminder")), OUTPUT_NAME)
                log.info("Alarm REMINDER: gas")
            elif (not gas_alarm) and prev_gas:
                alarm_active["gas"] = False
                last_alarm_emit["gas"] = now
                client.send_message_to_output(make_msg(alarm_event(device_id, "gas", gas, "cleared", severity="info")), OUTPUT_NAME)
                log.info("Alarm CLEARED: gas")

            # --- PIR alarm edge-triggered (tipo alarm) ---
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

            # --- Intrusion (solo si armado) edge-triggered ---
            intr = state["armed"] and is_intrusion(door, motion)
            if intr and not intrusion_active:
                intrusion_active = True
                client.send_message_to_output(make_msg(intrusion_event(device_id, door, motion, state["armed"])), OUTPUT_NAME)
                log.info("Event: INTRUSION")
            elif (not intr) and intrusion_active:
                intrusion_active = False
                # opcional: emitir cleared si quieres (ahora no, para no ensuciar)
                log.info("Intrusion cleared (internal)")

        # --- Aggregate cada AGG_PUB_SEC: foto + salud + estado de sensores ---
        if now - last_agg_pub_ts >= AGG_PUB_SEC:
            with lock:
                light_avg = avg(buf_light)
                temp_avg  = avg(buf_temp)
                hum_avg   = avg(buf_hum)
                buf_light.clear()
                buf_temp.clear()
                buf_hum.clear()

            uptime = int(now - start_ts)
            last_age = int(now - last_seen) if last_seen > 0 else None

            system = {
                "uptime_sec": uptime,
                "last_input_age_sec": last_age,
                "seq": seq_value,
                "armed": state["armed"]
            }

            sensors_payload = {
                "gas": {"alarm": alarm_active["gas"]},
                "pir": {"motion": state["motion"]},
                "door": {"state": state["door"]},
                "light": {"avg": light_avg, "state": state["light_bucket"], "last": state["light_analog"]},
                "temperature": {"avg": temp_avg, "state": state["temp_band"], "last": state["temp_c"]},
                "humidity": {"avg": hum_avg, "state": state["hum_band"], "last": state["hum"]}
            }

            evt = aggregate_event(device_id, AGG_PUB_SEC, system, sensors_payload)
            client.send_message_to_output(make_msg(evt), OUTPUT_NAME)
            log.info("Aggregate PUBLISHED to cloud")
            last_agg_pub_ts = now

        time.sleep(SAMPLE_TICK_SEC)

if __name__ == "__main__":
    main()
