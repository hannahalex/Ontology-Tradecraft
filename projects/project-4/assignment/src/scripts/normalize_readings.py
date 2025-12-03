import pandas as pd
import json
from dateutil import parser as dateparser
from pathlib import Path
import datetime

############# varibles and constants ##################

# Unit normalization
UNIT_MAP = {
 "F": "F",
    "°F": "F",
    "degF": "F",
    "C": "C",
    "°C": "C",
    "PSI": "psi",
    "psi": "psi",
    "kpa": "kPa",
    "KPA": "kPa",
    "kPa": "kPa",
    "pa": "Pa",
    "PA": "Pa",
    "Volt": "volt",
    "VOLT": "volt",
    "Ohm": "ohm",
    "OHM": "ohm",
}
#kinds from data to normalized kind
kind_map = {"temperature": "temperature", 
             "pressure": "pressure",
             "temp":"temperature",
             "resistance":"resistance",
             "voltage": "voltage"}



############ methods/functions ##############


def standardize_artifact_id(id: str) -> str: 
    if id is None:
        return None


def standardize_kind(kind: str) -> str: 
   if kind is None:
        return None

def standardize_unit(unit: str) -> str: 
   if unit is None:
        return None

def timestamp(time: str) -> pd.Timestamp:
   if time is None:
        return pd.NaT

#coordinated universal time UTC? according to test...
def time_to_utc(time: str) -> str:
    #  try:
    #     dt = dateparser.parse(str(x))
    #     if dt.tzinfo is None:
    #         dt = dt.replace(tzinfo=datetime.timezone.utc)
    #     return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    # except Exception:
    #     return None


    #df["timestamp"] = df["timestamp"].apply(to_iso8601)



############ noramlize functions ##############

def normalize_csv_sensor() -> pd.DataFrame:
    # df_a = pd.read_csv(IN_A, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])# Map columns to canonical names (EDIT to match the actual headers)
    # df_a = df_a.rename(columns={
    #     "Device Name": "artifact_id",
    #     "Reading Type": "sdc_kind",
    #     "Units": "unit_label",
    #     "Reading Value": "value",
    #     "Time (Local)": "timestamp",
    # })
    # # Keep only canonical columns that exist
    # df_a = df_a[[c for c in ["artifact_id","sdc_kind","unit_label","value","timestamp"] if c in df_a.columns]]

def normalize_json_sensor() -> pd.DataFrame:
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

def standardize_to_si(df):
    # PSI  to kPa
    mask_psi = df.unit_label == 'psi'
    df.loc[mask_psi, 'value'] = df.loc[mask_psi, 'value'] * 6.89476 * 1000
    df.loc[mask_psi, 'unit_label'] = 'Pa'

    #  kPa  to Pa
    mask_psi = df.unit_label == 'kPa'
    df.loc[mask_psi, 'value'] = df.loc[mask_psi, 'value'] * 1000
    df.loc[mask_psi, 'unit_label'] = 'Pa'
    # Convert to DataFrame
    df_b = pd.DataFrame(records)

    

def main(): 
    data_a  = Path("src/data/sensor_A.csv")
    data_b = Path("src/data/sensor_B.json")
    output = Path("src/data/readings_normalized.csv")

    #check if data_a and data_b exist
    df_a = normalize_csv_sensor(data_a)
    df_b = normalize_json_sensor(data_b)


    # get rid of empty data (NaN)
      #.dropna removes rows that contain null values
    df_a= df_a.dropna(subset=["artifact_id","sdc_kind","unit_label","value","timestamp"])
    df_b= df_b.dropna(subset=["artifact_id","sdc_kind","unit_label","value","timestamp"])

    for col in ["artifact_id","sdc_kind","unit_label"]:
        df[col] = df[col].astype(str).str.strip()


    #make one dataframe
    df = pd.concat([df_a, df_b], ignore_index=True)

    #making value into number type
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    #standardized unit label
    df["unit_label"] = df["unit_label"].map(standardize_unit)
    #df["unit_label"] = df["unit_label"].map(UNIT_MAP).fillna(df["unit_label"])


    #sort values
    df = df.sort_values(["artifact_id", "timestamp"]).reset_index(drop=True)

    #standardize units to SI
    df = standardize_to_si(df)

    #output dataframe
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(df)} rows.")


if __name__ == "__main__":
    main()