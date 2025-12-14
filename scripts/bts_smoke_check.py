"""
Quick smoke check for BTS-derived Parquet files.

Usage:
    python3 scripts/bts_smoke_check.py

This validates required columns exist, prints basic shapes,
date coverage, and a few top routes/carriers so you can confirm
the data looks sane before deeper modeling.
"""

import pathlib
import sys
import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
T100_PATH = BASE_DIR / "data" / "bts_t100.parquet"
DB1B_PATH = BASE_DIR / "data" / "db1b.parquet"


def load_or_fail(path: pathlib.Path) -> pd.DataFrame:
    if not path.exists():
        raise SystemExit(f"Missing file: {path}")
    return pd.read_parquet(path)


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
    t100 = load_or_fail(T100_PATH)
    db1b = load_or_fail(DB1B_PATH)

    summarize_df(
        t100,
        "T-100",
        ["year", "quarter", "carrier", "origin", "dest", "seats", "passengers", "distance", "departures"],
    )
    summarize_df(
        db1b,
        "DB1B",
        ["year", "quarter", "carrier", "origin", "dest", "passengers", "fare"],
    )

    top_routes(t100, "passengers", "T-100")
    top_routes(db1b, "passengers", "DB1B")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(1)
