"""
Quick smoke check for the raw BTS T-100 segment export (repo-root CSV).

Usage:
    python3 scripts/bts_smoke_check.py

Validates required columns exist, prints basic shapes, date coverage,
and a few top routes/carriers so you can confirm the data looks sane
before deeper modeling.
"""

import pathlib
import sys
import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
T100_PATH = BASE_DIR / "T_T100_SEGMENT_ALL_CARRIER.csv"
DB1B_PARQUET = BASE_DIR / "data" / "db1b.parquet"
DB1B_CSV = BASE_DIR / "data" / "db1b.csv"


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


def load_t100_segment(path: pathlib.Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Missing file: {path}")

    header = pd.read_csv(path, nrows=0)
    cols = set(header.columns)
    carrier_col = "UNIQUE_CARRIER" if "UNIQUE_CARRIER" in cols else "CARRIER"
    if carrier_col not in cols:
        raise SystemExit("T-100 file missing carrier column (expected UNIQUE_CARRIER or CARRIER).")

    departures_col = None
    for candidate in ("DEPARTURES_PERFORMED", "DEPARTURES_SCHEDULED"):
        if candidate in cols:
            departures_col = candidate
            break

    usecols = [
        c
        for c in (
            "YEAR",
            "QUARTER",
            carrier_col,
            "ORIGIN",
            "DEST",
            "SEATS",
            "PASSENGERS",
            "DISTANCE",
            departures_col,
        )
        if c and c in cols
    ]
    df = pd.read_csv(path, usecols=usecols)

    rename = {
        "YEAR": "year",
        "QUARTER": "quarter",
        carrier_col: "carrier",
        "ORIGIN": "origin",
        "DEST": "dest",
        "SEATS": "seats",
        "PASSENGERS": "passengers",
        "DISTANCE": "distance",
    }
    if departures_col:
        rename[departures_col] = "departures"
    df = df.rename(columns=rename)

    for col in ("year", "quarter", "seats", "passengers", "distance", "departures"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_db1b_optional() -> tuple[pathlib.Path | None, pd.DataFrame | None]:
    path = None
    df = None
    if DB1B_CSV.exists():
        path = DB1B_CSV
        df = pd.read_csv(DB1B_CSV)
    elif DB1B_PARQUET.exists():
        path = DB1B_PARQUET
        try:
            df = pd.read_parquet(DB1B_PARQUET)
        except Exception as exc:
            raise SystemExit(
                f"Failed to read {DB1B_PARQUET} ({exc}). "
                "Install a parquet engine (pyarrow/fastparquet) or provide data/db1b.csv instead."
            ) from exc

    if df is None:
        return None, None

    df = _canonicalize_columns(
        df,
        {
            "year": ["year", "YEAR"],
            "quarter": ["quarter", "QUARTER"],
            "carrier": ["carrier", "CARRIER", "ReportingCarrier", "MKT_CARRIER"],
            "origin": ["origin", "ORIGIN"],
            "dest": ["dest", "DEST", "destination"],
            "passengers": ["passengers", "PASSENGERS", "PAX"],
            "fare": ["fare", "MARKET_FARE", "FARE"],
        },
    )
    return path, df


def summarize_df(df: pd.DataFrame, name: str, required_cols):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise SystemExit(f"{name} is missing required columns: {missing}")
    print(f"== {name} ==")
    print(f"Rows: {len(df):,}")
    years = pd.to_numeric(df.get("year"), errors="coerce")
    quarters = pd.to_numeric(df.get("quarter"), errors="coerce")
    if not years.dropna().empty:
        print(f"Year range: {int(years.min())} - {int(years.max())}")
    if not quarters.dropna().empty:
        print(f"Quarter values: {sorted(quarters.dropna().unique().astype(int))}")
    carriers = df.get("carrier")
    if carriers is not None:
        print(f"Carriers: {carriers.nunique(dropna=True)}")
    print()


def top_routes(df: pd.DataFrame, metric: str, name: str, n: int = 5):
    if metric not in df.columns:
        return
    subset = (
        df.dropna(subset=["origin", "dest"])
        .groupby(["origin", "dest"], as_index=False)[metric]
        .sum()
        .sort_values(metric, ascending=False)
        .head(n)
    )
    print(f"Top {n} routes by {metric} ({name}):")
    print(subset.to_string(index=False))
    print()


def main():
    t100 = load_t100_segment(T100_PATH)
    db1b_path, db1b = load_db1b_optional()

    summarize_df(
        t100,
        "T-100",
        ["year", "quarter", "carrier", "origin", "dest", "seats", "passengers", "distance"],
    )
    if db1b is None:
        print("== DB1B ==\nMissing DB1B dataset (optional). Provide data/db1b.csv to enable.\n")
    else:
        summarize_df(
            db1b,
            f"DB1B ({db1b_path.name})",
            ["year", "quarter", "carrier", "origin", "dest", "passengers", "fare"],
        )

    top_routes(t100, "passengers", "T-100")
    if db1b is not None:
        top_routes(db1b, "passengers", "DB1B")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
