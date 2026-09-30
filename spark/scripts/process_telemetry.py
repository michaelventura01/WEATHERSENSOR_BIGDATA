from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    min,
    max,
    count,
    when,
    round
)

HDFS_INPUT = "hdfs://namenode:8020/iot/raw/telemetry"

HDFS_OUTPUT = (
    "hdfs://namenode:8020/iot/processed/telemetry_parquet"
)

spark = (
    SparkSession.builder
    .appName("IndustrialTelemetryProcessing")
    .getOrCreate()
)

print("========================================")
print(" INDUSTRIAL IoT - SPARK PROCESSING")
print("========================================")

print()
print("Reading data from HDFS...")
print(f"Input: {HDFS_INPUT}")
print()

# --------------------------------------------------
# 1. READ RAW DATA
# --------------------------------------------------

df_raw = (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(HDFS_INPUT)
)

print("RAW SCHEMA")
print("===========")

df_raw.printSchema()

print()
print("RAW DATA")
print("========")

df_raw.show(10, truncate=False)

# --------------------------------------------------
# 2. COUNT RECORDS
# --------------------------------------------------

total_records = df_raw.count()

print()
print(f"Total records: {total_records}")

# --------------------------------------------------
# 3. DATA CLEANING / TYPE CASTING
# --------------------------------------------------

df = (
    df_raw
    .select(
        col("device_id").cast("string"),
        col("machine_id").cast("string"),
        col("temperature").cast("double"),
        col("humidity").cast("double"),
        col("vibration").cast("double"),
        col("analog_value").cast("integer"),
        col("status").cast("string"),
        col("alarm").cast("boolean")
    )
)

# --------------------------------------------------
# 4. DATA QUALITY CHECK
# --------------------------------------------------

print()
print("DATA QUALITY")
print("============")

df.select(
    count("*").alias("total_records"),
    count(when(col("device_id").isNull(), 1))
        .alias("null_device_id"),
    count(when(col("machine_id").isNull(), 1))
        .alias("null_machine_id"),
    count(when(col("temperature").isNull(), 1))
        .alias("null_temperature"),
    count(when(col("humidity").isNull(), 1))
        .alias("null_humidity"),
    count(when(col("vibration").isNull(), 1))
        .alias("null_vibration")
).show()

# --------------------------------------------------
# 5. DETECT ANOMALIES
# --------------------------------------------------

df_processed = (
    df
    .withColumn(
        "temperature_anomaly",
        when(col("temperature") > 90, True)
        .otherwise(False)
    )
    .withColumn(
        "vibration_anomaly",
        when(col("vibration") > 10, True)
        .otherwise(False)
    )
    .withColumn(
        "calculated_anomaly",
        when(
            (col("temperature") > 90) |
            (col("vibration") > 10),
            True
        ).otherwise(False)
    )
)

print()
print("PROCESSED DATA")
print("==============")

df_processed.show(10, truncate=False)

# --------------------------------------------------
# 6. MACHINE STATISTICS
# --------------------------------------------------

print()
print("MACHINE STATISTICS")
print("==================")

df_processed.groupBy("machine_id").agg(
    count("*").alias("records"),
    round(avg("temperature"), 2)
        .alias("avg_temperature"),
    round(min("temperature"), 2)
        .alias("min_temperature"),
    round(max("temperature"), 2)
        .alias("max_temperature"),
    round(avg("humidity"), 2)
        .alias("avg_humidity"),
    round(avg("vibration"), 2)
        .alias("avg_vibration"),
    round(max("vibration"), 2)
        .alias("max_vibration"),
    count(
        when(col("calculated_anomaly") == True, 1)
    ).alias("anomalies")
).show(truncate=False)

# --------------------------------------------------
# 7. ANOMALY SUMMARY
# --------------------------------------------------

print()
print("ANOMALY SUMMARY")
print("===============")

df_processed.groupBy("status").count().show()

# --------------------------------------------------
# 8. WRITE PROCESSED DATA TO HDFS
# --------------------------------------------------

print()
print("Writing processed data to HDFS...")
print(f"Output: {HDFS_OUTPUT}")

(
    df_processed
    .write
    .mode("overwrite")
    .parquet(HDFS_OUTPUT)
)

print()
print("========================================")
print(" SPARK PROCESSING COMPLETED")
print("========================================")

spark.stop()