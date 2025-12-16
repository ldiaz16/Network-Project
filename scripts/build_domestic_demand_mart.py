#!/usr/bin/env python3
"""
Build the canonical Domestic Demand Mart (DB1B Market -> quarterly O&D).

Inputs:
  - A processed DB1B dataset (carrier+OD+quarter) such as:
      datasets/db1b/processed/db1b.parquet

Output schema (one row per year/quarter/origin/dest):
  - year, quarter, origin, dest, distance, passengers, avg_fare, fare_per_mile
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.bts_ingest import load_db1b


def _default_db1b_path(base_dir: Path) -> Path:
    candidates = [
        base_dir / "datasets" / "db1b" / "processed" / "db1b.parquet",
        base_dir / "data" / "db1b.parquet",
        base_dir / "datasets" / "db1b" / "processed" / "db1b.csv",
        base_dir / "data" / "db1b.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def build_domestic_demand_mart(db1b: pd.DataFrame) -> pd.DataFrame:
    required = {"year", "quarter", "origin", "destination", "passengers", "fare", "distance"}
    missing = sorted(required - set(db1b.columns))
    if missing:
        raise ValueError(f"DB1B input missing required columns: {', '.join(missing)}")

    df = db1b.copy()
    df["origin"] = df["origin"].astype(str).str.strip().str.upper()
    df["destination"] = df["destination"].astype(str).str.strip().str.upper()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce").astype("Int64")

    for col in ("passengers", "fare", "distance"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[(df["year"].notna()) & (df["quarter"].between(1, 4))].copy()
    df = df[(df["origin"] != "") & (df["destination"] != "")].copy()

    df["revenue_proxy"] = df["fare"] * df["passengers"]
    df["distance_x_pax"] = df["distance"] * df["passengers"]

    grouped = (
        df.groupby(["year", "quarter", "origin", "destination"], as_index=False)
        .agg(
            passengers=("passengers", "sum"),
            revenue_proxy=("revenue_proxy", "sum"),
            distance_x_pax=("distance_x_pax", "sum"),
        )
        .reset_index(drop=True)
    )

    denom = grouped["passengers"].where(grouped["passengers"] > 0, pd.NA)
    grouped["avg_fare"] = grouped["revenue_proxy"] / denom
    grouped["distance"] = grouped["distance_x_pax"] / denom
    grouped["fare_per_mile"] = grouped["avg_fare"] / grouped["distance"].where(grouped["distance"] > 0, pd.NA)

    out = grouped.rename(columns={"destination": "dest"}).drop(columns=["revenue_proxy", "distance_x_pax"])
    out = out.sort_values(["year", "quarter", "origin", "dest"], kind="mergesort").reset_index(drop=True)
    return out


def _write_output(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = out_path.suffix.lower()
    if suffix == ".parquet":
        df.to_parquet(out_path, index=False, compression="zstd")
        return
    if suffix == ".csv":
        df.to_csv(out_path, index=False)
        return
    raise SystemExit(f"Unsupported output format: {out_path} (expected .parquet or .csv)")


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Build quarterly Domestic Demand Mart from processed DB1B.")
    parser.add_argument("--db1b-path", type=Path, default=_default_db1b_path(base_dir))
    parser.add_argument(
        "--out",
        type=Path,
        default=base_dir / "datasets" / "db1b" / "processed" / "domestic_demand_mart.parquet",
    )
    parser.add_argument(
        "--rolling-quarters",
        type=int,
        default=0,
        help="0 disables filtering; otherwise keeps last N quarters (based on latest in the file).",
    )
    args = parser.parse_args()

    db1b_path: Path = args.db1b_path
    if not db1b_path.exists():
        raise SystemExit(f"Missing DB1B dataset: {db1b_path}")

    df = load_db1b(
        str(db1b_path),
        rolling_quarters=max(0, int(args.rolling_quarters)),
        domestic_only=True,
    )
    if df.empty:
        raise SystemExit("No DB1B rows after filtering.")

    mart = build_domestic_demand_mart(df)
    _write_output(mart, args.out)

    years = pd.to_numeric(mart["year"], errors="coerce")
    quarters = pd.to_numeric(mart["quarter"], errors="coerce")
    print(f"Wrote: {args.out.relative_to(base_dir) if args.out.is_relative_to(base_dir) else args.out}")
    print(f"Rows: {len(mart):,}")
    print(f"Year range: {int(years.min())} - {int(years.max())}")
    print(f"Quarter values: {sorted(quarters.dropna().unique().astype(int).tolist())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

