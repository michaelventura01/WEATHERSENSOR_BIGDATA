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
# INFLUXDB
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
# KAFKA
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
print("       KAFKA → INFLUXDB")
print("========================================")
print(f"Kafka broker : {KAFKA_BROKER}")
print(f"Kafka topic  : {KAFKA_TOPIC}")
print(f"InfluxDB     : {INFLUX_URL}")
print(f"Organization : {INFLUX_ORG}")
print(f"Bucket       : {INFLUX_BUCKET}")
print()
print("Waiting for Kafka messages...")
print()


try:

    for message in consumer:

        data = message.value

        print("----------------------------------------")
        print("Kafka message received:")
        print(json.dumps(data, indent=2))

        try:

            point = (
                Point("machine_telemetry")

                .tag(
                    "device_id",
                    str(data["device_id"])
                )

                .tag(
                    "machine_id",
                    str(data["machine_id"])
                )

                .tag(
                    "status",
                    str(data["status"])
                )

                .field(
                    "temperature",
                    float(data["temperature"])
                )

                .field(
                    "humidity",
                    float(data["humidity"])
                )

                .field(
                    "vibration",
                    float(data["vibration"])
                )

                .field(
                    "analog_value",
                    int(data["analog_value"])
                )

                .field(
                    "alarm",
                    bool(data["alarm"])
                )

                .time(datetime.now(timezone.utc))
            )

            write_api.write(
                bucket=INFLUX_BUCKET,
                org=INFLUX_ORG,
                record=point,
            )

            print("InfluxDB write: SUCCESS")

            print(
                f"Machine     : {data['machine_id']}"
            )

            print(
                f"Temperature : {data['temperature']} °C"
            )

            print(
                f"Humidity    : {data['humidity']} %"
            )

            print(
                f"Vibration   : {data['vibration']} mm/s"
            )

            print(
                f"Status      : {data['status']}"
            )

            print(
                f"Alarm       : {data['alarm']}"
            )

        except Exception as exc:

            print(
                f"ERROR processing message: {exc}"
            )

        print("----------------------------------------")


except KeyboardInterrupt:

    print()
    print("Stopping Kafka → InfluxDB consumer...")


finally:

    consumer.close()
    write_api.close()
    influx_client.close()

    print("Consumer stopped.")
