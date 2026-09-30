import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "factory.machine.telemetry"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "RXPbv8uiO_jGUtafjLMWe3jyC92qF69oKOLrAO9aRhkbFJY8Zl5abn9XO09uAzltGEQ_RVKByFyklmTWuDmd6g=="
INFLUX_ORG = "iot-bigdata"
INFLUX_BUCKET = "machine_telemetry"


# ============================================================
# INFLUXDB CLIENT
# ============================================================

influx_client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG,
)

write_api = influx_client.write_api(
    write_options=SYNCHRONOUS
)


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="iot-influx-consumer",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)


print("========================================")
print("KAFKA → INFLUXDB")
print("========================================")
print(f"Kafka broker : {KAFKA_BROKER}")
print(f"Kafka topic  : {KAFKA_TOPIC}")
print(f"InfluxDB     : {INFLUX_URL}")
print(f"Organization : {INFLUX_ORG}")
print(f"Bucket       : {INFLUX_BUCKET}")
print()
print("Waiting for Kafka messages...")
print()


# ============================================================
# PROCESS MESSAGES
# ============================================================

try:

    for message in consumer:

        data = message.value

        print("----------------------------------------")
        print("Kafka message received:")
        print(json.dumps(data, indent=2))

        try:

            device_id = data["device_id"]
            machine_id = data["machine_id"]

            temperature = float(data["temperature"])
            humidity = float(data["humidity"])
            vibration = float(data["vibration"])
            analog_value = int(data["analog_value"])

            status = data["status"]
            alarm = bool(data["alarm"])

            # ------------------------------------------------
            # Create InfluxDB point
            # ------------------------------------------------

            point = (
                Point("machine_telemetry")

                # Tags
                .tag("device_id", device_id)
                .tag("machine_id", machine_id)
                .tag("status", status)

                # Fields
                .field("temperature", temperature)
                .field("humidity", humidity)
                .field("vibration", vibration)
                .field("analog_value", analog_value)
                .field("alarm", alarm)

                # Timestamp
                .time(datetime.now(timezone.utc))
            )

            # ------------------------------------------------
            # Write to InfluxDB
            # ------------------------------------------------

            write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=point,
            )

            print("InfluxDB write: SUCCESS")
            print(f"Machine      : {machine_id}")
            print(f"Temperature  : {temperature} °C")
            print(f"Humidity     : {humidity} %")
            print(f"Vibration    : {vibration} mm/s")
            print(f"Status       : {status}")
            print(f"Alarm        : {alarm}")

        except Exception as exc:

            print(f"ERROR processing message: {exc}")

        print("----------------------------------------")


except KeyboardInterrupt:

    print()
    print("Stopping Kafka → InfluxDB consumer...")


finally:

    consumer.close()
    write_api.close()
    influx_client.close()

    print("Consumer stopped.")
