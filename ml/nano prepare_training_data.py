import pandas as pd
import glob
import os

INPUT_PATH = "output/telemetry_parquet/*.parquet"
OUTPUT_FILE = "output/telemetry_training.parquet"

print("========================================")
print(" INDUSTRIAL IoT - PREPARE ML DATA")
print("========================================")

files = glob.glob(INPUT_PATH)

print()
print("Parquet files found:")

for file in files:
    print(file)

if not files:
    raise FileNotFoundError(
        "No Parquet files found. Verify the Spark output."
    )

frames = []

for file in files:
    df_part = pd.read_parquet(file)
    frames.append(df_part)

df = pd.concat(
    frames,
    ignore_index=True
)

print()
print("Total records:")
print(len(df))

print()
print("Columns:")
print(df.columns.tolist())

print()
print("Dataset preview:")
print(df.head(10))

os.makedirs("output", exist_ok=True)

df.to_parquet(
    OUTPUT_FILE,
    index=False
)

print()
print("Training dataset created:")
print(OUTPUT_FILE)

print()
print("========================================")
print(" DATA PREPARATION COMPLETED")
print("========================================")