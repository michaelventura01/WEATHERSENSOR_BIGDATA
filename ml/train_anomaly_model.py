import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import os

INPUT_FILE = "output/telemetry_training.parquet"

MODEL_DIR = "models"
OUTPUT_FILE = "output/ml_predictions.parquet"

FEATURES = [
    "temperature",
    "humidity",
    "vibration",
    "analog_value"
]

print("========================================")
print(" INDUSTRIAL IoT - MACHINE LEARNING")
print("========================================")

print()
print("Loading training data...")
print(f"Input: {INPUT_FILE}")

df = pd.read_parquet(INPUT_FILE)

print()
print("Records:", len(df))

print()
print("Columns:")
print(df.columns.tolist())

missing_columns = [
    column
    for column in FEATURES
    if column not in df.columns
]

if missing_columns:
    raise ValueError(
        f"Missing columns: {missing_columns}"
    )

# ----------------------------------------
# SELECT FEATURES
# ----------------------------------------

X = df[FEATURES].copy()

# Handle missing values
X = X.fillna(X.median())

print()
print("ML FEATURES:")
print(X)

# ----------------------------------------
# STANDARDIZATION
# ----------------------------------------

scaler = StandardScaler()

X_scaled = scaler.fit_transform(X)

# ----------------------------------------
# ISOLATION FOREST
# ----------------------------------------

model = IsolationForest(
    n_estimators=100,
    contamination=0.10,
    random_state=42
)

model.fit(X_scaled)

# ----------------------------------------
# PREDICTIONS
# ----------------------------------------

df["ml_prediction"] = model.predict(X_scaled)

df["ml_anomaly"] = (
    df["ml_prediction"] == -1
)

df["anomaly_score"] = (
    model.decision_function(X_scaled)
)

df["ml_prediction_label"] = (
    df["ml_anomaly"]
    .map({
        True: "ANOMALY",
        False: "NORMAL"
    })
)

# ----------------------------------------
# RESULTS
# ----------------------------------------

print()
print("========================================")
print(" ML RESULTS")
print("========================================")

print()

print(
    df[
        FEATURES
        + [
            "ml_prediction_label",
            "anomaly_score"
        ]
    ].to_string(index=False)
)

print()
print("Prediction summary:")

print(
    df["ml_prediction_label"]
    .value_counts()
)

# ----------------------------------------
# SAVE MODEL
# ----------------------------------------

os.makedirs(MODEL_DIR, exist_ok=True)

joblib.dump(
    model,
    f"{MODEL_DIR}/isolation_forest.joblib"
)

joblib.dump(
    scaler,
    f"{MODEL_DIR}/scaler.joblib"
)

# ----------------------------------------
# SAVE PREDICTIONS
# ----------------------------------------

df.to_parquet(
    OUTPUT_FILE,
    index=False
)

print()
print("========================================")
print(" MACHINE LEARNING COMPLETED")
print("========================================")

print()
print("Model:")
print(f"{MODEL_DIR}/isolation_forest.joblib")

print()
print("Scaler:")
print(f"{MODEL_DIR}/scaler.joblib")

print()
print("Predictions:")
print(OUTPUT_FILE)