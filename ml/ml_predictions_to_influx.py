import pandas as pd
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


INPUT_FILE = "output/ml_predictions.parquet"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "RXPbv8uiO_jGUtafjLMWe3jyC92qF69oKOLrAO9aRhkbFJY8Zl5abn9XO09uAzltGEQ_RVKByFyklmTWuDmd6g=="
INFLUX_ORG = "iot-bigdata"
INFLUX_BUCKET = "machine_telemetry"


print("========================================")
print(" MACHINE LEARNING → INFLUXDB")
print("========================================")

df = pd.read_parquet(INPUT_FILE)

print()
print(f"Records: {len(df)}")

client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)

write_api = client.write_api(
    write_options=SYNCHRONOUS
)


for _, row in df.iterrows():

    point = (
        Point("ml_predictions")

        .tag(
            "machine_id",
            str(row["machine_id"])
        )

        .tag(
            "device_id",
            str(row["device_id"])
        )

        .tag(
            "prediction",
            str(row["ml_prediction_label"])
        )

        .field(
            "anomaly_score",
            float(row["anomaly_score"])
        )

        .field(
            "ml_anomaly",
            bool(row["ml_anomaly"])
        )

        .field(
            "temperature",
            float(row["temperature"])
        )

        .field(
            "humidity",
            float(row["humidity"])
        )

        .field(
            "vibration",
            float(row["vibration"])
        )

        .time(
            datetime.now(timezone.utc)
        )
    )

    write_api.write(
        bucket=INFLUX_BUCKET,
        org=INFLUX_ORG,
        record=point
    )


print()
print("ML predictions written to InfluxDB.")

write_api.close()
client.close()

print()
print("========================================")
print(" COMPLETED")
print("========================================")