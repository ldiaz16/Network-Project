from pathlib import Path

import pandas as pd
from geopy.distance import geodesic
from data.airlines import normalize_name
from data.aircraft_config import AIRLINE_SEAT_CONFIG

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR_ROOT = BASE_DIR / "data"
LOOKUP_DIR = BASE_DIR / "Lookup Tables"

def load_airlines(path: str = None) -> pd.DataFrame:
    """
    Load airline metadata.

    Defaults to BTS carrier lookup tables (T-100 compatible). Pass a `path` to load
    a custom schema instead.
    """
    if path:
        data_path = Path(path)
        df = pd.read_csv(data_path, header=None)
        df = df.iloc[:, :7]
        df.columns = ["Airline", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active"]
        df["Airline (Normalized)"] = df["Airline"].map(normalize_name)
        return df

    lookup_path = LOOKUP_DIR / "L_UNIQUE_CARRIERS.csv"
    raw = pd.read_csv(lookup_path, dtype=str)
    df = pd.DataFrame(
        {
            "Airline": raw["Description"].astype(str).str.strip().str.strip('"'),
            "Alias": pd.NA,
            "IATA": raw["Code"].astype(str).str.strip().str.strip('"').str.upper(),
            "ICAO": pd.NA,
            "Callsign": pd.NA,
            "Country": pd.NA,
            "Active": "Y",
        }
    )
    df["Airline (Normalized)"] = df["Airline"].map(normalize_name)
    return df

def load_airports(path: str = None) -> pd.DataFrame:
    """
    Load airport metadata.

    Defaults to BTS airport lookup tables (T-100 compatible, no coordinates). Pass a
    `path` to load a custom schema instead.
    """
    columns = [
        "Airport ID",
        "Name",
        "City",
        "Country",
        "IATA",
        "ICAO",
        "Latitude",
        "Longitude",
        "Altitude",
        "Timezone",
        "DST",
        "Tz",
        "Type",
        "Source",
    ]

    if path:
        data_path = Path(path)
        df = pd.read_csv(data_path, header=None)
        df = df.iloc[:, :14]
        df.columns = columns
        return df

    lookup_path = LOOKUP_DIR / "L_AIRPORT.csv"
    raw = pd.read_csv(lookup_path, dtype=str)
    codes = raw["Code"].astype(str).str.strip().str.strip('"').str.upper()
    desc = raw["Description"].astype(str).str.strip()

    parts = desc.str.partition(":")
    location = parts[0].astype(str).str.strip()
    airport_name = parts[2].astype(str).str.strip()
    airport_name = airport_name.where(airport_name != "", location)
    city = location.str.split(",", n=1).str[0].str.strip()
    region = location.str.split(",", n=1).str[1].fillna("").str.strip()
    country = region.where(region != "", pd.NA)

    df = pd.DataFrame(
        {
            "Airport ID": pd.NA,
            "Name": airport_name.where(airport_name.astype(str).str.len() > 0, pd.NA),
            "City": city.where(city.astype(str).str.len() > 0, pd.NA),
            "Country": country,
            "IATA": codes,
            "ICAO": pd.NA,
            "Latitude": pd.NA,
            "Longitude": pd.NA,
            "Altitude": pd.NA,
            "Timezone": pd.NA,
            "DST": pd.NA,
            "Tz": pd.NA,
            "Type": "airport",
            "Source": "BTS",
        }
    )
    return df

def load_routes(path: str = None, rolling_quarters: int = 4, domestic_only: bool = True) -> pd.DataFrame:
    """
    Load route-level aggregates.

    Defaults to the BTS T-100 segment export in the repo root
    (`T_T100_SEGMENT_ALL_CARRIER.csv` or `T_T100_SEGMENT_ALL_CARRIER.csv.gz`).

    If `path` is provided, it is treated as an alternate T-100 segment CSV.
    """
    if path:
        t100_segments_path = Path(path)
    else:
        t100_segments_path = BASE_DIR / "T_T100_SEGMENT_ALL_CARRIER.csv"
        if not t100_segments_path.exists():
            gz_path = BASE_DIR / "T_T100_SEGMENT_ALL_CARRIER.csv.gz"
            if gz_path.exists():
                t100_segments_path = gz_path
    if not t100_segments_path.exists():
        raise FileNotFoundError(f"Missing T-100 segment file: {t100_segments_path}")

    header = pd.read_csv(t100_segments_path, nrows=0)
    columns = set(header.columns)
    carrier_col = "UNIQUE_CARRIER" if "UNIQUE_CARRIER" in columns else "CARRIER"
    if carrier_col is None or carrier_col not in columns:
        raise ValueError("T-100 segment file is missing a carrier column.")

    required_cols = {carrier_col, "ORIGIN", "DEST", "SEATS"}
    missing = [col for col in required_cols if col not in columns]
    if missing:
        raise ValueError(f"T-100 segment file is missing columns: {', '.join(missing)}")

    usecols = [
        col
        for col in [
            carrier_col,
            "ORIGIN",
            "DEST",
            "SEATS",
            "PASSENGERS",
            "DISTANCE",
            "AIRCRAFT_TYPE",
            "YEAR",
            "QUARTER",
            "ORIGIN_STATE_ABR",
            "DEST_STATE_ABR",
        ]
        if col in columns
    ]
    raw = pd.read_csv(t100_segments_path, usecols=usecols)

    def _clean_str(series: pd.Series) -> pd.Series:
        return series.astype(str).str.strip().str.upper().replace({"\\N": ""})

    raw[carrier_col] = _clean_str(raw[carrier_col])
    raw["ORIGIN"] = _clean_str(raw["ORIGIN"])
    raw["DEST"] = _clean_str(raw["DEST"])

    if rolling_quarters and "YEAR" in raw.columns and "QUARTER" in raw.columns:
        years = pd.to_numeric(raw["YEAR"], errors="coerce")
        quarters = pd.to_numeric(raw["QUARTER"], errors="coerce")
        quarter_index = years * 4 + quarters
        latest = quarter_index.max()
        if pd.notna(latest) and latest > 0:
            window = max(1, int(rolling_quarters))
            min_index = latest - (window - 1)
            raw = raw.loc[quarter_index >= min_index].copy()

    if domestic_only and {"ORIGIN_STATE_ABR", "DEST_STATE_ABR"}.issubset(raw.columns):
        # Best-effort domestic-only filter using BTS state/province lookup if available.
        state_lookup = LOOKUP_DIR / "L_STATE_ABR_AVIATION.csv"
        if state_lookup.exists():
            try:
                state_df = pd.read_csv(state_lookup, dtype=str)
                if {"Code", "Description"}.issubset(state_df.columns):
                    mapping = {}
                    for _, row in state_df.iterrows():
                        code = str(row.get("Code") or "").strip().strip('"').upper()
                        desc = str(row.get("Description") or "").strip()
                        if not code:
                            continue
                        lowered = desc.lower()
                        if "canada" in lowered:
                            mapping[code] = "Canada"
                        elif "mexico" in lowered:
                            mapping[code] = "Mexico"
                        else:
                            mapping[code] = "United States"
                    us_states = {code for code, country in mapping.items() if country == "United States"}
                    origin_state = _clean_str(raw["ORIGIN_STATE_ABR"]).str.strip('"')
                    dest_state = _clean_str(raw["DEST_STATE_ABR"]).str.strip('"')
                    raw = raw.loc[origin_state.isin(us_states) & dest_state.isin(us_states)].copy()
            except Exception:
                pass

    code_pattern = r"^[A-Z0-9]{2,5}$"
    valid_mask = (
        raw[carrier_col].astype(str).str.match(code_pattern)
        & raw["ORIGIN"].astype(str).str.match(code_pattern)
        & raw["DEST"].astype(str).str.match(code_pattern)
    )
    raw = raw.loc[valid_mask].copy()

    for numeric in ("SEATS", "PASSENGERS", "DISTANCE"):
        if numeric in raw.columns:
            raw[numeric] = pd.to_numeric(raw[numeric], errors="coerce").fillna(0.0)

    def _most_common(series: pd.Series):
        cleaned = series.dropna().astype(str)
        if cleaned.empty:
            return ""
        mode = cleaned.mode()
        return mode.iloc[0] if not mode.empty else ""

    grouped = (
        raw.groupby([carrier_col, "ORIGIN", "DEST"], dropna=False)
        .agg(
            Total=("SEATS", "sum"),
            **({"Passengers": ("PASSENGERS", "sum")} if "PASSENGERS" in raw.columns else {}),
            **({"Distance (reported)": ("DISTANCE", "mean")} if "DISTANCE" in raw.columns else {}),
            **({"Equipment": ("AIRCRAFT_TYPE", _most_common)} if "AIRCRAFT_TYPE" in raw.columns else {}),
        )
        .reset_index()
    )
    grouped.rename(
        columns={
            carrier_col: "Airline Code",
            "ORIGIN": "Source airport",
            "DEST": "Destination airport",
        },
        inplace=True,
    )
    grouped["Seat Source"] = "t100_segments"
    if "Equipment" not in grouped.columns:
        grouped["Equipment"] = ""
    grouped["Equipment Code"] = grouped["Equipment"]
    return grouped

def filter_codeshare_routes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove codeshare-only rows from a routes dataframe.
    Some route datasets mark codeshares with a literal ``Y`` in the ``Codeshare`` column.
    """
    if df is None or df.empty or "Codeshare" not in df.columns:
        return df
    normalized = (
        df["Codeshare"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"\\N": ""})
    )
    mask = normalized == "Y"
    if not mask.any():
        return df
    filtered = df.loc[~mask].copy()
    filtered.reset_index(drop=True, inplace=True)
    return filtered

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
