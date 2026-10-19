import json
import random
import time
import uuid
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = "localhost"
MQTT_PORT = 1883

SENSOR_ID = "traffic_001"
MQTT_TOPIC = f"urbanpulse/traffic/{SENSOR_ID}"


client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id=SENSOR_ID
)

client.connect(MQTT_HOST, MQTT_PORT)
client.loop_start()


try:
    while True:
        event = {
            "event_id": str(uuid.uuid4()),
            "sensor_id": SENSOR_ID,
            "sensor_type": "traffic",
            "event_time": datetime.now(timezone.utc).isoformat(),
            "value": random.randint(0, 100),
            "units": "vehicles",
            "city_zone": "downtown"
        }

        payload = json.dumps(event)


        client.publish(
            MQTT_TOPIC,
            payload
        )

        print(payload)

        time.sleep(2)

finally:
    client.loop_stop()
    client.disconnect()