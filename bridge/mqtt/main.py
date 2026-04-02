import json
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from azure.iot.device import IoTHubModuleClient

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "building/01/home/+/telemetry")
EDGE_OUTPUT = os.getenv("EDGE_OUTPUT", "telemetry")

edge_client = IoTHubModuleClient.create_from_edge_environment()

def utc_now():
    return datetime.now(timezone.utc).isoformat()

def is_sensorsim_like(data: dict) -> bool:
    s = data.get("sensors", {})
    if not isinstance(s, dict):
        return False
    return any(isinstance(s.get(k), dict) for k in ("gas","sound","light","pir","dht11"))

def to_sensorsim_like(data: dict) -> dict:
    # Si ya viene en formato estable, lo dejamos tal cual
    if is_sensorsim_like(data):
        return data

    device_id = data.get("deviceId", "unknown")
    s = data.get("sensors", {}) if isinstance(data.get("sensors"), dict) else {}

    motion = bool(s.get("motion", False))
    gas_bool = bool(s.get("gas", False))
    ldr_raw = s.get("ldr_raw", None)
    temp = s.get("temperature", None)
    hum = s.get("humidity", None)

    # PRESERVAR door/touch desde NodeMCU
    door = s.get("door", "UNKNOWN")
    if not isinstance(door, str):
        door = "UNKNOWN"
    touch = bool(s.get("touch", False))

    out = {
        "deviceId": device_id,
        "ts": data.get("ts") or utc_now(),
        "sensors": {
            "gas": {"analog": None, "alarm": gas_bool},
            "sound": {"analog": None, "alarm": False},
            "light": {"analog": ldr_raw if isinstance(ldr_raw, (int, float)) else None},
            "pir": {"motion": motion},
            "dht11": {
                "temp_c": temp if isinstance(temp, (int, float)) else None,
                "humidity": hum if isinstance(hum, (int, float)) else None
            }
        },
        "_meta": {"door": door, "touch": touch}
    }
    return out

def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] Connected rc={rc}")
    client.subscribe(MQTT_TOPIC)
    print(f"[MQTT] Subscribed {MQTT_TOPIC}")

def on_message(client, userdata, msg):
    raw = msg.payload.decode("utf-8")
    data_in = json.loads(raw)
    data_out = to_sensorsim_like(data_in)

    edge_client.send_message_to_output(json.dumps(data_out), EDGE_OUTPUT)
    print("[EDGE] Forwarded (normalized + meta)")

mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
mqtt_client.loop_forever()
