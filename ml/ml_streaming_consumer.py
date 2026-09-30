import json
from datetime import datetime, timezone

import joblib
import numpy as np
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "factory.machine.telemetry"
KAFKA_GROUP = "iot-ml-consumer"

MODEL_FILE = "models/isolation_forest.joblib"
SCALER_FILE = "models/scaler.joblib"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "RXPbv8uiO_jGUtafjLMWe3jyC92qF69oKOLrAO9aRhkbFJY8Zl5abn9XO09uAzltGEQ_RVKByFyklmTWuDmd6g=="
INFLUX_ORG = "iot-bigdata"
INFLUX_BUCKET = "machine_telemetry"


# ============================================================
# LOAD MODEL
# ============================================================

print("========================================")
print(" INDUSTRIAL IoT - ML STREAMING")
print("========================================")
print()

print("Loading ML model...")
model = joblib.load(MODEL_FILE)

print("Loading scaler...")
scaler = joblib.load(SCALER_FILE)

print("Model loaded successfully.")
print()


# ============================================================
# KAFKA
# ============================================================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id=KAFKA_GROUP,
    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)


# ============================================================
# INFLUXDB
# ============================================================

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(
    write_options=SYNCHRONOUS
)


print("Kafka broker :", KAFKA_BROKER)
print("Kafka topic  :", KAFKA_TOPIC)
print("Consumer     :", KAFKA_GROUP)
print("InfluxDB     :", INFLUX_URL)
print()
print("Waiting for telemetry...")
print()


# ============================================================
# PROCESS STREAM
# ============================================================

try:

    for message in consumer:

        data = message.value

        print("----------------------------------------")
        print("NEW TELEMETRY")
        print("----------------------------------------")

        print(json.dumps(data, indent=2))

        # ----------------------------------------------------
        # Extract features
        # ----------------------------------------------------

        temperature = float(data["temperature"])
        humidity = float(data["humidity"])
        vibration = float(data["vibration"])
        analog_value = int(data["analog_value"])

        # ----------------------------------------------------
        # Create feature matrix
        # ----------------------------------------------------

        X = np.array([
            [
                temperature,
                humidity,
                vibration,
                analog_value
            ]
        ])

        # ----------------------------------------------------
        # Scale
        # ----------------------------------------------------

        X_scaled = scaler.transform(X)

        # ----------------------------------------------------
        # ML prediction
        # ----------------------------------------------------

        prediction = int(
            model.predict(X_scaled)[0]
        )

        anomaly_score = float(
            model.decision_function(X_scaled)[0]
        )

        if prediction == -1:

            ml_anomaly = True
            prediction_label = "ANOMALY"

        else:

            ml_anomaly = False
            prediction_label = "NORMAL"

        # ----------------------------------------------------
        # Display result
        # ----------------------------------------------------

        print()
        print("ML RESULT")
        print("----------------------------------------")
        print(f"Temperature  : {temperature:.2f} °C")
        print(f"Humidity     : {humidity:.2f} %")
        print(f"Vibration    : {vibration:.2f} mm/s")
        print(f"Analog value : {analog_value}")
        print()
        print(f"Prediction   : {prediction_label}")
        print(f"Anomaly      : {ml_anomaly}")
        print(f"Score        : {anomaly_score:.6f}")

        # ----------------------------------------------------
        # Write prediction to InfluxDB
        # ----------------------------------------------------

        point = (
            Point("ml_predictions")

            .tag(
                "machine_id",
                str(data["machine_id"])
            )

            .tag(
                "device_id",
                str(data["device_id"])
            )

            .tag(
                "prediction",
                prediction_label
            )

            .field(
                "anomaly_score",
                anomaly_score
            )

            .field(
                "ml_anomaly",
                ml_anomaly
            )

            .field(
                "temperature",
                temperature
            )

            .field(
                "humidity",
                humidity
            )

            .field(
                "vibration",
                vibration
            )

            .time(datetime.now(timezone.utc))
        )

        write_api.write(
            bucket=INFLUX_BUCKET,
            org=INFLUX_ORG,
            record=point
        )

        print()
        print("InfluxDB write: SUCCESS")
        print("----------------------------------------")
        print()


except KeyboardInterrupt:

    print()
    print("Stopping ML streaming consumer...")


finally:

    consumer.close()
    write_api.close()
    client.close()

    print("ML streaming consumer stopped.")