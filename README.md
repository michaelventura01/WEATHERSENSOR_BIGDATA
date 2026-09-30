# Industrial IoT & Big Data – Predictive Maintenance

End-to-end academic project integrating **IoT, MQTT, Apache Kafka,
Hadoop HDFS, Apache Spark, Machine Learning, InfluxDB, Grafana, and
Docker** for industrial telemetry monitoring and anomaly detection.

## Objective

The project simulates an industrial machine using an ESP32 in Wokwi.
Sensor telemetry is transmitted through MQTT and Kafka, stored in a
Hadoop Data Lake, processed with Spark, analyzed with an Isolation
Forest model, stored in InfluxDB, and visualized through Grafana.

The system supports both batch processing and near-real-time ML
inference.

## Architecture

``` text
Wokwi ESP32
    |
    | MQTT
    v
Mosquitto
    |
    v
Apache Kafka
    |
    +--------------------+
    |                    |
    v                    v
HDFS Data Lake       ML Streaming
    |                    |
    v                    v
Apache Spark         Isolation Forest
    |                    |
    v                    v
Batch ML             InfluxDB
                         |
                         v
                      Grafana
```

## Technology Stack

| Layer                | Technology              |
|----------------------|-------------------------|
| IoT simulation       | Wokwi                   |
| Microcontroller      | ESP32                   |
| Sensors              | DHT22 + potentiometer   |
| Messaging            | MQTT                    |
| MQTT broker          | Eclipse Mosquitto       |
| Streaming            | Apache Kafka            |
| Data Lake            | Hadoop HDFS             |
| Processing           | Apache Spark            |
| Machine Learning     | Python / Scikit-learn   |
| Anomaly detection    | Isolation Forest        |
| Time-series database | InfluxDB                |
| Visualization        | Grafana                 |
| Containers           | Docker / Docker Compose |
| Languages            | C++, Python, Flux       |

## Repository Structure

``` text
iot-bigdata-project/
├── data/
├── docs/
├── grafana/
├── hdfs/
│   ├── config/
│   └── scripts/
├── influxdb/
├── kafka/
├── ml/
│   ├── models/
│   ├── output/
│   ├── prepare_training_data.py
│   ├── train_anomaly_model.py
│   ├── ml_predictions_to_influx.py
│   └── ml_streaming_consumer.py
├── mqtt/
└── simulator/
    └── SENSOR_TEMPERATURA/
```

## IoT Simulator

The ESP32 is simulated with Wokwi.

### Sensors

- **DHT22**: temperature and humidity.
- **Potentiometer**: used as a simplified vibration sensor.
- **LED**: local anomaly/alarm indicator.

The simulated vibration is calculated as:

``` text
vibration = (analog_value / 4095) × 15
```

The ESP32 generates an anomaly when:

``` text
temperature > 90 °C
OR
vibration > 10 mm/s
```

### MQTT Topic

``` text
factory/machine/MACHINE_001/telemetry
```

Example payload:

``` json
{
  "device_id": "ESP32_MACHINE_001",
  "machine_id": "MACHINE_001",
  "temperature": 24.0,
  "humidity": 40.0,
  "vibration": 15.0,
  "analog_value": 4095,
  "status": "ANOMALY",
  "alarm": true
}
```

## MQTT → Kafka

The Python bridge subscribes to the MQTT topic and publishes telemetry
to:

``` text
factory.machine.telemetry
```

Kafka broker:

``` text
localhost:9092
```

The bridge validates JSON and reports Kafka topic, partition, and offset
for successful messages.

## Kafka → HDFS

The Kafka consumer stores raw telemetry in HDFS using date/hour
organization:

``` text
/iot/raw/telemetry/YYYY/MM/DD/hour=HH/
```

This creates the raw Data Lake layer for historical processing.

## Spark Processing

Spark reads the raw JSON telemetry from:

``` text
hdfs://namenode:8020/iot/raw/telemetry
```

Processing includes:

1.  Schema normalization.
2.  Data-quality checks.
3.  Temperature anomaly calculation.
4.  Vibration anomaly calculation.
5.  Combined anomaly calculation.
6.  Machine-level statistics.
7.  Parquet output.

Output:

``` text
hdfs://namenode:8020/iot/processed/telemetry_parquet
```

## Machine Learning

The anomaly-detection model uses:

``` text
temperature
humidity
vibration
analog_value
```

Pipeline:

``` text
Parquet
  ↓
Pandas
  ↓
Feature selection
  ↓
StandardScaler
  ↓
IsolationForest
  ↓
Prediction + anomaly score
```

Generated artifacts:

``` text
ml/models/isolation_forest.joblib
ml/models/scaler.joblib
```

Predictions include:

- `ml_prediction`
- `ml_anomaly`
- `anomaly_score`
- `ml_prediction_label`

Labels:

``` text
NORMAL
ANOMALY
```

## ML Streaming Inference

`ml_streaming_consumer.py` consumes new Kafka telemetry and applies the
trained model to each incoming record.

Flow:

``` text
Kafka
  ↓
ML Consumer
  ↓
StandardScaler
  ↓
Isolation Forest
  ↓
Anomaly prediction
  ↓
InfluxDB
```

The streaming consumer uses:

``` text
iot-ml-consumer
```

and writes ML results to:

``` text
ml_predictions
```

## InfluxDB

Main bucket:

``` text
machine_telemetry
```

Measurements include:

``` text
machine_telemetry
ml_predictions
```

The ML measurement stores:

``` text
temperature
humidity
vibration
ml_anomaly
anomaly_score
```

Tags include:

``` text
machine_id
device_id
prediction
```

## Grafana

Dashboard:

``` text
Industrial IoT Monitoring
```

The dashboard contains monitoring and ML views including:

- Machine Temperature
- Machine Vibration
- Machine Humidity
- Machine Alarm
- Vibration Sensor Raw Value
- Machine Status
- ML Machine Status
- ML Anomaly Score
- Latest ML Anomaly Score
- Current Temperature
- Current Vibration

Recommended live-test range:

``` text
Last 15 minutes
```

or:

``` text
Last 1 hour
```

## Docker Services

Main containers:

``` text
iot-mosquitto
iot-kafka
iot-influxdb
iot-hdfs-namenode
iot-hdfs-datanode
iot-spark
iot-grafana
```

Check:

``` bash
docker ps
```

## Starting the Infrastructure

### Mosquitto

``` bash
cd mqtt
docker compose up -d
```

### Kafka

``` bash
cd kafka
docker compose up -d
```

### InfluxDB

``` bash
cd influxdb
docker compose up -d
```

### HDFS

``` bash
cd hdfs
docker compose up -d
```

### Spark

``` bash
cd spark
docker compose up -d
```

### Grafana

``` bash
cd grafana
docker compose up -d
```

Verify:

``` bash
docker ps
```

## Starting the Processing Services

### MQTT → Kafka

``` bash
cd kafka
source .venv/bin/activate
python mqtt_to_kafka.py
```

### Kafka → HDFS

``` bash
cd hdfs/scripts
source ../../kafka/.venv/bin/activate
python kafka_to_hdfs.py
```

### ML Streaming

``` bash
cd ml
source .venv/bin/activate
python ml_streaming_consumer.py
```

Keep these processes running during the integration test.

## End-to-End Integration Test

### Test 1 — Normal

Set the Wokwi potentiometer so:

``` text
vibration < 10 mm/s
```

Expected:

``` text
Alarm = false
Status = NORMAL
```

Expected pipeline:

``` text
ESP32
 → MQTT
 → Kafka
 → ML
 → InfluxDB
 → Grafana
```

### Test 2 — Anomaly

Increase the potentiometer until:

``` text
vibration > 10 mm/s
```

Example:

``` text
vibration ≈ 15 mm/s
analog_value = 4095
```

Expected:

``` text
Alarm = true
Status = ANOMALY
```

The ML streaming layer should produce:

``` text
prediction = ANOMALY
ml_anomaly = true
```

and write the result to InfluxDB.

## Validation Checklist

### IoT

- [x] ESP32 simulation
- [x] DHT22 temperature
- [x] DHT22 humidity
- [x] Vibration simulation
- [x] Local alarm indicator

### MQTT

- [x] Mosquitto running
- [x] ESP32 connection
- [x] MQTT telemetry

### Kafka

- [x] Kafka broker
- [x] Telemetry topic
- [x] MQTT → Kafka bridge

### Data Lake

- [x] HDFS NameNode
- [x] HDFS DataNode
- [x] Kafka → HDFS ingestion
- [x] Date/hour organization

### Spark

- [x] HDFS ingestion
- [x] Data-quality validation
- [x] Anomaly calculations
- [x] Parquet output

### Machine Learning

- [x] Training dataset
- [x] StandardScaler
- [x] Isolation Forest
- [x] Model serialization
- [x] Batch predictions
- [x] Streaming inference

### InfluxDB

- [x] Telemetry storage
- [x] ML prediction storage
- [x] `ml_predictions` measurement

### Grafana

- [x] InfluxDB data source
- [x] Industrial IoT dashboard
- [x] Sensor visualizations
- [x] ML anomaly score
- [x] ML status visualization

## Academic Scope and Limitations

This is a functional academic prototype. The Isolation Forest model
demonstrates the anomaly-detection workflow using simulated telemetry.

The current dataset is relatively small and generated from a simulated
environment. It should not be considered a production-ready
predictive-maintenance model.

A production implementation would require:

- Larger historical datasets.
- Real sensor telemetry.
- Sensor calibration.
- Failure labels where available.
- Feature engineering.
- Model validation.
- Hyperparameter optimization.
- Model monitoring.
- Retraining strategy.
- Security and authentication.
- High availability.

## Future Improvements

Possible extensions:

- Real vibration sensors.
- Pressure, RPM, and current sensors.
- Raspberry Pi edge gateway.
- Multiple simulated machines.
- Kafka topic partitioning by machine.
- Schema Registry.
- Kafka Connect.
- Spark Structured Streaming.
- Online model retraining.
- Grafana alerting.
- Notification integration.
- Cloud deployment.
- Kubernetes deployment.

## Educational Value

The project demonstrates the integration of:

``` text
IoT
+
MQTT
+
Kafka
+
HDFS
+
Spark
+
Machine Learning
+
InfluxDB
+
Grafana
```

It shows how raw industrial telemetry can be transformed into monitoring
and anomaly-detection information using streaming, Big Data processing,
machine learning, and visualization.

## Author

**Michael Ventura**  
Solutions Architect

## Project Status

**Functional academic prototype**

Implemented components:

- IoT simulation
- MQTT communication
- Kafka streaming
- HDFS Data Lake
- Spark processing
- Isolation Forest anomaly detection
- ML streaming inference
- InfluxDB storage
- Grafana monitoring
