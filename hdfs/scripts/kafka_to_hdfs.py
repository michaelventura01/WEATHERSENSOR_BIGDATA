import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer


KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "factory.machine.telemetry"

HDFS_CONTAINER = "iot-hdfs-namenode"
HDFS_BASE_PATH = "/iot/raw/telemetry"


def write_to_hdfs(data):

    now = datetime.now(timezone.utc)

    date_path = now.strftime("%Y/%m/%d")
    hour = now.strftime("%H")

    hdfs_directory = (
        f"{HDFS_BASE_PATH}/"
        f"{date_path}/"
        f"hour={hour}"
    )

    filename = (
        f"telemetry_"
        f"{now.strftime('%Y%m%d_%H%M%S_%f')}.json"
    )

    hdfs_file = f"{hdfs_directory}/{filename}"

    # Create HDFS directory
    result = os.system(
        f"docker exec {HDFS_CONTAINER} "
        f"hdfs dfs -mkdir -p {hdfs_directory}"
    )

    if result != 0:
        print("ERROR: Could not create HDFS directory")
        return

    # Temporary local file
    local_file = f"/tmp/{filename}"

    with open(local_file, "w") as f:
        json.dump(data, f)
        f.write("\n")

    # Copy file into NameNode container
    result = os.system(
        f"docker cp {local_file} "
        f"{HDFS_CONTAINER}:/tmp/{filename}"
    )

    if result != 0:
        print("ERROR: docker cp failed")
        os.remove(local_file)
        return

    # Upload into HDFS
    result = os.system(
        f"docker exec {HDFS_CONTAINER} "
        f"hdfs dfs -put "
        f"/tmp/{filename} "
        f"{hdfs_file}"
    )

    # Cleanup
    os.remove(local_file)

    os.system(
        f"docker exec {HDFS_CONTAINER} "
        f"rm -f /tmp/{filename}"
    )

    if result == 0:
        print("----------------------------------------")
        print("HDFS WRITE: SUCCESS")
        print(f"HDFS path: {hdfs_file}")
        print("----------------------------------------")
    else:
        print("ERROR: Failed to write data to HDFS")


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="iot-hdfs-consumer",
    value_deserializer=lambda value:
        json.loads(value.decode("utf-8"))
)


print("========================================")
print("       KAFKA -> HDFS CONSUMER")
print("========================================")
print(f"Kafka broker : {KAFKA_BROKER}")
print(f"Kafka topic  : {KAFKA_TOPIC}")
print(f"HDFS path    : {HDFS_BASE_PATH}")
print()
print("Waiting for Kafka messages...")
print()


try:

    for message in consumer:

        data = message.value

        print("----------------------------------------")
        print("Kafka message received")
        print(f"Partition : {message.partition}")
        print(f"Offset    : {message.offset}")

        print(json.dumps(data, indent=2))

        write_to_hdfs(data)


except KeyboardInterrupt:

    print()
    print("Stopping Kafka -> HDFS consumer...")


finally:

    consumer.close()
    print("Consumer stopped.")