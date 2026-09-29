import json
import time

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


# ============================================================
# CONFIGURATION
# ============================================================

MQTT_BROKER = "localhost"
MQTT_PORT = 1883

MQTT_TOPIC = "factory/machine/MACHINE_001/telemetry"

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "factory.machine.telemetry"


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


# ============================================================
# MQTT CALLBACKS
# ============================================================

def on_connect(client, userdata, flags, reason_code, properties=None):
    print("========================================")
    print("MQTT → KAFKA BRIDGE")
    print("========================================")

    print(f"MQTT broker : {MQTT_BROKER}:{MQTT_PORT}")
    print(f"MQTT topic  : {MQTT_TOPIC}")
    print(f"Kafka broker: {KAFKA_BROKER}")
    print(f"Kafka topic : {KAFKA_TOPIC}")
    print()

    if reason_code == 0:
        print("MQTT connection: SUCCESS")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to: {MQTT_TOPIC}")
    else:
        print(f"MQTT connection failed: {reason_code}")


def on_message(client, userdata, msg):

    print("----------------------------------------")

    print("MQTT message received")
    print(f"Topic: {msg.topic}")

    try:
        payload = json.loads(msg.payload.decode("utf-8"))

        print("Payload:")
        print(json.dumps(payload, indent=2))

        # Send message to Kafka
        future = producer.send(
            KAFKA_TOPIC,
            value=payload,
        )

        # Wait for Kafka acknowledgement
        metadata = future.get(timeout=10)

        print("Kafka publish: SUCCESS")
        print(f"Kafka topic     : {metadata.topic}")
        print(f"Kafka partition : {metadata.partition}")
        print(f"Kafka offset    : {metadata.offset}")

        producer.flush()

    except json.JSONDecodeError:
        print("ERROR: Invalid JSON received from MQTT")

    except Exception as exc:
        print(f"ERROR sending message to Kafka: {exc}")

    print("----------------------------------------")


def on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    print(f"MQTT disconnected. Reason: {reason_code}")


# ============================================================
# MQTT CLIENT
# ============================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="mqtt-kafka-bridge",
)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_disconnect = on_disconnect


# ============================================================
# CONNECT
# ============================================================

print("Starting MQTT → Kafka bridge...")

while True:

    try:

        mqtt_client.connect(
            MQTT_BROKER,
            MQTT_PORT,
            keepalive=60,
        )

        mqtt_client.loop_forever()

    except KeyboardInterrupt:

        print()
        print("Stopping bridge...")

        break

    except Exception as exc:

        print(f"Connection error: {exc}")
        print("Retrying in 5 seconds...")

        time.sleep(5)


producer.flush()
producer.close()

print("Bridge stopped.")
