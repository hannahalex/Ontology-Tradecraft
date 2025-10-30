import pandas as pd
import json
from dateutil import parser as dateparser
from pathlib import Path
import datetime


#Define input/output locations
IN_A = Path("src/data/sensor_A.csv")
IN_B = Path("src/data/sensor_B.json")
OUT  = Path("src/data/readings_normalized.csv")


#Load Sensor A (CSV)
df_a = pd.read_csv(IN_A, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])# Map columns to canonical names (EDIT to match the actual headers)
df_a = df_a.rename(columns={
    "Device Name": "artifact_id",
    "Reading Type": "sdc_kind",
    "Units": "unit_label",
    "Reading Value": "value",
    "Time (Local)": "timestamp",
    
})
# Keep only canonical columns that exist
df_a = df_a[[c for c in ["artifact_id","sdc_kind","unit_label","value","timestamp"] if c in df_a.columns]]


# Load the sensor_B.json file
sensor_b_path =  Path("src/data/sensor_B.json")
with open(sensor_b_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Extract structured records from the nested JSON
records = []
for reading in data.get("readings", []):
    entity_id = reading.get("entity_id")
    for entry in reading.get("data", []):
        kind = entry.get("kind")
        value = entry.get("value")
        unit = entry.get("unit")
        timestamp = entry.get("time")
        records.append({
            "artifact_id": entity_id,
            "sdc_kind": kind,
            "unit_label": unit,
            "value": value,
            "timestamp": timestamp
        })

# Convert to DataFrame
df_b = pd.DataFrame(records)

# Display the first few rows of the parsed DataFrame
df_b.head()

# Concatenate A + B
df = pd.concat([df_a, df_b], ignore_index=True)


#Trim whitespace + basic nornmalization
for col in ["artifact_id","sdc_kind","unit_label"]:
    df[col] = df[col].astype(str).str.strip()

# numeric
df["value"] = pd.to_numeric(df["value"], errors="coerce")

#Timestamp parsing to ISO 8601
def to_iso8601(x):
    try:
        dt = dateparser.parse(str(x))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None

    
df["timestamp"] = df["timestamp"].apply(to_iso8601)


# Unit normalization
UNIT_MAP = {
    "celsius": "C", "°c": "C", "C": "C",
    "kilogram": "kg", "KG": "kg", "kg": "kg",
    "meter": "m", "M": "m", "m": "m",
    "pressure": "kPa", "psi":"kPa", "kpa": "kPa", "Kpa":"kPa", "kPa":"kPa", "KPA":"kPa"
}
df["unit_label"] = df["unit_label"].str.lower().map(UNIT_MAP).fillna(df["unit_label"])


# Drop rows with missing critical values
df = df.dropna(subset=["artifact_id","sdc_kind","unit_label","value","timestamp"])


# Sort for readability (optional)
df = df.sort_values(["artifact_id", "timestamp"]).reset_index(drop=True)

# Write output
OUT.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT, index=False)
print(f"Wrote {OUT} with {len(df)} rows.")