from pathlib import Path

import pandas as pd
from geopy.distance import geodesic
from .airlines import normalize_name
from .aircraft_config import AIRLINE_SEAT_CONFIG

DATA_DIR_ROOT = Path(__file__).resolve().parents[1] / "data"

def load_airlines(path: str = None) -> pd.DataFrame:
    data_path = Path(path) if path else DATA_DIR_ROOT / "airlines.dat"
    df = pd.read_csv(data_path, header=None)
    df.columns = ["Airline","Alias","IATA","ICAO","Callsign","Country","Active"]
    df["Airline (Normalized)"] = df["Airline"].map(normalize_name)
    return df

def load_airports(path: str = None) -> pd.DataFrame:
    data_path = Path(path) if path else DATA_DIR_ROOT / "airports.dat"
    df = pd.read_csv(data_path, header=None)
    df.columns = ["Airport ID","Name","City","Country","IATA","ICAO","Latitude","Longitude",
                  "Altitude","Timezone","DST","Tz","Type","Source"]
    return df

def load_routes(path: str = None) -> pd.DataFrame:
    data_path = Path(path) if path else DATA_DIR_ROOT / "routes.dat"
    df = pd.read_csv(data_path, header=None)
    df.columns = ["Airline Code","IDK","Source airport","Source airport ID",
                  "Destination airport","Destination airport ID","Codeshare","Stops","Equipment"]
    return df

def load_cbsa(path: str = None) -> pd.DataFrame:
    data_path = Path(path) if path else DATA_DIR_ROOT / "cbsa.csv"
    return pd.read_csv(data_path, skiprows=2)

def convert_aircraft_config_to_df(config_dict=AIRLINE_SEAT_CONFIG) -> pd.DataFrame:
    rows = []
    for airline, types in config_dict.items():
        for ac, seats in types.items():
            rows.append({
                "Airline": airline,
                "Aircraft": ac,
                **seats
            })
    return pd.DataFrame(rows)

def compute_distance(lat1, lon1, lat2, lon2):
    try:
        return geodesic((lat1, lon1), (lat2, lon2)).miles
    except Exception:
        return 0.0
