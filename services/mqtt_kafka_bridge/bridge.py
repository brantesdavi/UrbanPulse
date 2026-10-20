import json

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "urbanpulse/traffic/#"

KAFK_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sensor_events"


producer = Producer({
    "bootstrap.servers": KAFK_BOOTSTRAP_SERVERS
})

def on_connect(client, userdata, flags, reaseon_code, properties):
    print(f"Connected to MQTT broker: {reaseon_code}")

    client.subscribe(MQTT_TOPIC)

    print(f"Subscribed to: {MQTT_TOPIC}")


def on_message(client, userdata, message):

    payload = message.payload.decode("utf-8")

    event = json.loads(payload)

    sensor_id = event["sensor_id"]

    producer.produce(
        topic = KAFKA_TOPIC,
        key=sensor_id,
        value=payload,
        callback=delivery_report
    )

    producer.poll(0)

    # print(
    #     f"MQTT -> Kafka | sensor={sensor_id}"
    # )

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="urbanpulse-mqtt-kafka-bridge"
)

def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Kafka delivered | "
            f"topic={msg.topic()} | "
            f"partition={msg.partition()} | "
            f"offset={msg.offset()}"
        )

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

mqtt_client.connect(
    MQTT_HOST,
    MQTT_PORT
)

print("Bridge started")

mqtt_client.loop_forever()
