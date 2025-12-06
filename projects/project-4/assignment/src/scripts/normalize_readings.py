import pandas as pd
import json
from dateutil import parser as dateparser
from pathlib import Path
import datetime
from datetime import datetime
import pytz

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
    id =str(id).strip()

    parts = id.split()
    id = " ".join(parts)

    id = id.replace( " ", "-")
    return id


def standardize_kind(kind: str) -> str: 
   if kind is None:
        return None
   new_kind = str(kind).strip().lower()
   key = kind_map.get(new_kind )

   #if kind is none, get the row and remove from data
   return key 


def standardize_unit(unit: str) -> str: 
   if unit is None:
        return None
   unit = str(unit).strip()
   mapped_key = UNIT_MAP.get(unit, UNIT_MAP.get(unit.upper(),UNIT_MAP.get(unit)) )
   return mapped_key


def timestamp(time: str) -> pd.Timestamp:
   if time is None:
        return pd.NaT
   ts = pd.to_datetime(time, utc=True).strftime()
   return ts

#coordinated universal time UTC? according to test...
def time_to_utc(time: str) -> str:
    #defined timezone for buffalo
    our_timezone = pytz.timezone('America/New_York')

    #local time string
    naive_dt = datetime.strptime(time,"%m/%d/%y %H:%M")

    #localize buffalo timezone
    local = our_timezone.localize(naive_dt)

    #convert to UTC
    utc = local.astimezone(pytz.UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


    # dt = dateparser.parse(str(x))
    # try:
    #     if dt.tzinfo is None:
    #         # You can choose a policy; here we treat naive as UTC
    #         import datetime, pytz
    #         dt = dt.replace(tzinfo=datetime.timezone.utc)
    #     return dt.astimezone(datetime.timezone.utc).isoformat().replace("+00:00","Z")
    # except Exception:
    #     return None
    return utc

def standardize_value(x): 
    try:
        if x is None or "":
            return None
        return float(x)
    except Exception: 
        return None

############ noramlize functions ##############

def normalize_csv_sensor(path_a) -> pd.DataFrame:
    df_a = pd.read_csv(path_a, dtype=str, keep_default_na=False, na_values=["", "NA", "NaN"])# Map columns to canonical names (EDIT to match the actual headers)
    
    df_a = df_a.rename(
        columns={
        "Device Name": "artifact_id",
        "Reading Type": "sdc_kind",
        "Units": "unit_label",
        "Reading Value": "value",
        "Time (Local)": "timestamp",
    })
    # Keep only canonical columns that exist
    df_a = df_a[[c for c in ["artifact_id","sdc_kind","unit_label","value","timestamp"] if c in df_a.columns]]
    
    #standardize
    df_a["artifact_id"] = df_a["artifact_id"].map(standardize_artifact_id)
    df_a["sdc_kind"] = df_a["sdc_kind"].map(standardize_kind)
    df_a["value"] = df_a["value"].map(standardize_value)
    df_a["timestamp"] = df_a["timestamp"].map(time_to_utc)
    
    return df_a

def normalize_json_sensor(path_b) -> pd.DataFrame:
     # Load the sensor_B.json file
    path_b = Path(path_b)
    with open(path_b, "r", encoding="utf-8") as f:
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
    df_b = pd.DataFrame(records)
    return df_b

def standardize_si(df):
    f_value = df.unit_label == 'F'
    df.loc[f_value, 'value'] = (df.loc[f_value, 'value'] - 32) * 5 / 9
    df.loc[f_value, 'unit_label'] = 'C'
   
    # PSI  to kPa
    psi_value = df.unit_label == 'psi'
    df.loc[psi_value, 'value'] = df.loc[psi_value, 'value'] * 6.89476 * 1000
    df.loc[psi_value, 'unit_label'] = 'Pa'

    #  kPa  to Pa
    kpa_value = df.unit_label == 'kPa'
    df.loc[kpa_value, 'value'] = df.loc[kpa_value, 'value'] * 1000
    df.loc[kpa_value, 'unit_label'] = 'Pa'
    
    return df 

def main(): 
    data_a  = Path("src/data/sensor_A.csv")
    data_b = Path("src/data/sensor_B.json")
    OUT = Path("src/data/readings_normalized.csv")

    #check if data_a and data_b exist
    df_a = normalize_csv_sensor(data_a)
    df_b = normalize_json_sensor(data_b)


    # get rid of empty data (NaN)
      #.dropna removes rows that contain null values
    df_a= df_a.dropna(subset=["artifact_id","sdc_kind","unit_label","value","timestamp"])
    df_b= df_b.dropna(subset=["artifact_id","sdc_kind","unit_label","value","timestamp"])

    #make one dataframe
    df = pd.concat([df_a, df_b], ignore_index=True)

    for col in ["artifact_id","sdc_kind","unit_label"]:
        df[col] = df[col].astype(str).str.strip()

    #making value into number type
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    #standardized unit label
    df["unit_label"] = df["unit_label"].map(standardize_unit)
    #df["unit_label"] = df["unit_label"].map(UNIT_MAP).fillna(df["unit_label"])

    #sort values
    df = df.sort_values(["artifact_id", "timestamp"]).reset_index(drop=True)

    #standardize units to SI
    df = standardize_si(df)

    #output data frame
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(df)} rows.")


if __name__ == "__main__":
    main()