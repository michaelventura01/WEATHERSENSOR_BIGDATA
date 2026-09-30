import pandas as pd
import glob

files = glob.glob(
    "output/telemetry_parquet/*.parquet"
)

print("Parquet files found:")
for file in files:
    print(file)

if not files:
    raise FileNotFoundError(
        "No Parquet files found."
    )

frames = []

for file in files:
    frames.append(
        pd.read_parquet(file)
    )

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

df.to_parquet(
    "output/telemetry_training.parquet",
    index=False
)

print()
print("Training dataset created:")
print(
    "output/telemetry_training.parquet"
)