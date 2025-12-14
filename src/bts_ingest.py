import pandas as pd
from typing import Tuple, Optional, List


def _read_frame(path: str) -> pd.DataFrame:
    if path.lower().endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _canonicalize_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    cols = {col.lower(): col for col in df.columns}
    rename = {}
    for target, candidates in mapping.items():
        for cand in candidates:
            key = cand.lower()
            if key in cols:
                rename[cols[key]] = target
                break
    return df.rename(columns=rename)


def _latest_quarter(df: pd.DataFrame, year_col: str = "year", quarter_col: str = "quarter") -> Optional[Tuple[int, int]]:
    if df.empty or year_col not in df or quarter_col not in df:
        return None
    latest_year = df[year_col].max()
    latest_quarter = df[df[year_col] == latest_year][quarter_col].max()
    try:
        return int(latest_year), int(latest_quarter)
    except Exception:
        return None


def _rolling_quarters(latest: Tuple[int, int], window: int = 4) -> List[Tuple[int, int]]:
    year, quarter = latest
    quarters = []
    for _ in range(window):
        quarters.append((year, quarter))
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    return quarters


def load_t100(path: str, rolling_quarters: int = 4, domestic_only: bool = True) -> pd.DataFrame:
    """Load and normalize BTS T-100 segment/market data."""
    df = _read_frame(path)
    df = _canonicalize_columns(
        df,
        {
            "year": ["YEAR", "Year"],
            "quarter": ["QUARTER", "Quarter"],
            "carrier": ["UNIQUE_CARRIER", "CARRIER", "Carrier"],
            "origin": ["ORIGIN", "Origin"],
            "destination": ["DEST", "Destination"],
            "departures": ["DEPARTURES_PERFORMED", "DEPARTURES_SCHEDULED", "Departures"],
            "seats": ["SEATS", "Seats"],
            "passengers": ["PASSENGERS", "Passengers"],
            "distance": ["DISTANCE", "Distance"],
        },
    )
    # Drop rows without routing context
    df = df.dropna(subset=["origin", "destination"])
    for col in ("year", "quarter"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("departures", "seats", "passengers", "distance"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    latest = _latest_quarter(df, "year", "quarter")
    if latest:
        target_quarters = set(_rolling_quarters(latest, window=rolling_quarters))
        df = df[df.apply(lambda row: (int(row["year"]), int(row["quarter"])) in target_quarters, axis=1)]

    if domestic_only and "domestic" in (c.lower() for c in df.columns):
        # If a domestic flag exists, use it; otherwise assume input already filtered.
        domestic_col = [c for c in df.columns if c.lower() == "domestic"][0]
        df = df[df[domestic_col] == 1]

    return df.reset_index(drop=True)


def load_db1b(path: str, rolling_quarters: int = 4, domestic_only: bool = True) -> pd.DataFrame:
    """Load and normalize DB1B O&D data (ticket sample)."""
    df = _read_frame(path)
    df = _canonicalize_columns(
        df,
        {
            "year": ["YEAR", "Year"],
            "quarter": ["QUARTER", "Quarter"],
            "carrier": ["CARRIER", "ReportingCarrier", "Carrier", "MKT_CARRIER"],
            "origin": ["ORIGIN", "Origin"],
            "destination": ["DEST", "Destination"],
            "passengers": ["PASSENGERS", "Passengers", "PAX"],
            "fare": ["MARKET_FARE", "FARE", "Fare"],
            "distance": ["MARKET_MILES_FLOWN", "DISTANCE", "Distance", "MARKET_DISTANCE"],
        },
    )
    df = df.dropna(subset=["origin", "destination"])
    for col in ("year", "quarter"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("passengers", "fare", "distance"):
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    latest = _latest_quarter(df, "year", "quarter")
    if latest:
        target_quarters = set(_rolling_quarters(latest, window=rolling_quarters))
        df = df[df.apply(lambda row: (int(row["year"]), int(row["quarter"])) in target_quarters, axis=1)]

    if domestic_only and "domestic" in (c.lower() for c in df.columns):
        domestic_col = [c for c in df.columns if c.lower() == "domestic"][0]
        df = df[df[domestic_col] == 1]

    return df.reset_index(drop=True)


def build_profitability_table(t100: Optional[pd.DataFrame], db1b: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Merge T-100 and DB1B to estimate revenue, RASM, CASM proxy, and a profitability score."""
    if t100 is None or t100.empty:
        return None

    t100_df = t100.copy()
    numeric_cols = ["departures", "seats", "passengers", "distance"]
    for col in numeric_cols:
        if col in t100_df:
            t100_df[col] = pd.to_numeric(t100_df[col], errors="coerce").fillna(0.0)

    # Aggregate T-100 by carrier/OD
    t_grouped = (
        t100_df.groupby(["carrier", "origin", "destination"], dropna=False)
        .agg(
            departures=("departures", "sum"),
            seats=("seats", "sum"),
            passengers=("passengers", "sum"),
            distance=("distance", "mean"),
        )
        .reset_index()
    )
    t_grouped["distance"] = t_grouped["distance"].fillna(0.0)
    t_grouped["asm"] = (t_grouped["seats"] * t_grouped["distance"]).clip(lower=0.0)

    db_df = None
    if db1b is not None and not db1b.empty:
        db_df = db1b.copy()
        for col in ("passengers", "fare", "distance"):
            if col in db_df:
                db_df[col] = pd.to_numeric(db_df[col], errors="coerce").fillna(0.0)
        db_df = (
            db_df.groupby(["carrier", "origin", "destination"], dropna=False)
            .apply(
                lambda g: pd.Series(
                    {
                        "db_passengers": g["passengers"].sum(),
                        "avg_fare": (g["fare"] * g["passengers"]).sum() / g["passengers"].sum() if g["passengers"].sum() > 0 else 0.0,
                        "db_distance": (g["distance"] * g["passengers"]).sum() / g["passengers"].sum() if "distance" in g else None,
                    }
                )
            )
            .reset_index()
        )

    merged = t_grouped
    if db_df is not None:
        merged = merged.merge(db_df, on=["carrier", "origin", "destination"], how="left")

    merged["db_passengers"] = merged.get("db_passengers", 0.0).fillna(0.0)
    merged["avg_fare"] = merged.get("avg_fare", 0.0).fillna(0.0)
    merged["revenue"] = merged["db_passengers"] * merged["avg_fare"]

    # Yield/RASM proxy
    distance_basis = merged["distance"].where(merged["distance"] > 0, merged.get("db_distance")).fillna(0.0)
    merged["yield_per_mile"] = merged.apply(
        lambda row: (row["avg_fare"] / distance_basis.loc[row.name]) if distance_basis.loc[row.name] > 0 else 0.0,
        axis=1,
    )
    merged["rasm"] = merged.apply(
        lambda row: (row["revenue"] / row["asm"]) if row.get("asm", 0) > 0 else 0.0,
        axis=1,
    )

    def _casm_proxy(stage_length: float) -> float:
        # Simple declining CASM curve with distance; floor at 4 cents.
        if stage_length <= 0:
            return 0.09
        return max(0.04, 0.14 - 0.05 * min(stage_length / 2000.0, 1.0))

    merged["casm_proxy"] = distance_basis.apply(_casm_proxy)
    merged["profit_score"] = merged["rasm"] - merged["casm_proxy"]
    merged["pdews"] = merged["db_passengers"] / 365.0 / (4 / 4)  # simple daily average over 4 quarters

    return merged
