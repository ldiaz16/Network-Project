#!/usr/bin/env python3
"""
Quick, simple summaries from the processed DB1B dataset.

Examples:
  python3 scripts/db1b_quicklook.py
  python3 scripts/db1b_quicklook.py --carrier DL --top-n 15
  python3 scripts/db1b_quicklook.py --rolling-quarters 0 --year 2024 --quarter 2
  python3 scripts/db1b_quicklook.py --min-passengers 500 --sort yield_per_mile
"""

from __future__ import annotations

import argparse
import os
import re
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


def _default_lookup_dir(base_dir: Path) -> Path:
    candidates = [
        base_dir / "datasets" / "db1b" / "raw" / "lookup_tables",
        base_dir / "Lookup Tables",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _load_lookup_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    df = pd.read_csv(path, usecols=["Code", "Description"])
    df["Code"] = df["Code"].astype(str).str.strip().str.upper()
    df["Description"] = df["Description"].astype(str).fillna("")
    return dict(zip(df["Code"], df["Description"]))


_STATE_RE = re.compile(r",\s*([A-Z]{2})\s*:")


def _extract_state(description: str) -> str | None:
    if not description:
        return None
    match = _STATE_RE.search(description)
    return match.group(1) if match else None


def _add_lookup_columns(
    df: pd.DataFrame,
    *,
    airport_desc: dict[str, str],
    carrier_desc: dict[str, str],
) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "carrier" in out.columns and carrier_desc:
        out["carrier_name"] = out["carrier"].map(carrier_desc)
    if {"origin", "destination"} <= set(out.columns) and airport_desc:
        out["origin_desc"] = out["origin"].map(airport_desc)
        out["dest_desc"] = out["destination"].map(airport_desc)
        out["origin_state"] = out["origin_desc"].map(_extract_state)
        out["dest_state"] = out["dest_desc"].map(_extract_state)
    return out


def _summarize_routes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["revenue_proxy"] = tmp["fare"] * tmp["passengers"]
    tmp["distance_x_pax"] = tmp["distance"] * tmp["passengers"]

    grouped = (
        tmp.groupby(["origin", "destination"], as_index=False)
        .agg(
            passengers=("passengers", "sum"),
            revenue_proxy=("revenue_proxy", "sum"),
            distance_x_pax=("distance_x_pax", "sum"),
        )
        .reset_index(drop=True)
    )
    grouped["avg_fare"] = grouped["revenue_proxy"] / grouped["passengers"].where(grouped["passengers"] > 0, 1)
    grouped["avg_distance"] = grouped["distance_x_pax"] / grouped["passengers"].where(grouped["passengers"] > 0, 1)
    grouped["yield_per_mile"] = grouped["avg_fare"] / grouped["avg_distance"].where(grouped["avg_distance"] > 0, pd.NA)
    return grouped


def _summarize_carriers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["revenue_proxy"] = tmp["fare"] * tmp["passengers"]
    grouped = (
        tmp.groupby(["carrier"], as_index=False)
        .agg(
            passengers=("passengers", "sum"),
            revenue_proxy=("revenue_proxy", "sum"),
        )
        .reset_index(drop=True)
    )
    grouped["avg_fare"] = grouped["revenue_proxy"] / grouped["passengers"].where(grouped["passengers"] > 0, 1)
    return grouped


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="DB1B quicklook (top routes, yields, carrier mix).")
    parser.add_argument("--db1b-path", type=Path, default=_default_db1b_path(base_dir))
    parser.add_argument("--lookup-dir", type=Path, default=_default_lookup_dir(base_dir))
    parser.add_argument("--carrier", type=str, default=None, help="Reporting carrier code (e.g., DL, AA).")
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--quarter", type=int, default=None)
    parser.add_argument("--rolling-quarters", type=int, default=4, help="0 disables filtering; app default is 4.")
    parser.add_argument("--top-n", type=int, default=15)
    parser.add_argument("--min-passengers", type=float, default=0.0)
    parser.add_argument(
        "--sort",
        type=str,
        default="passengers",
        choices=["passengers", "revenue_proxy", "avg_fare", "yield_per_mile"],
        help="Metric to sort routes by.",
    )
    args = parser.parse_args()

    db1b_path: Path = args.db1b_path
    if not db1b_path.exists():
        raise SystemExit(f"Missing DB1B dataset: {db1b_path}")

    lookup_dir: Path = args.lookup_dir
    airport_desc = _load_lookup_map(lookup_dir / "L_AIRPORT.csv")
    carrier_desc = _load_lookup_map(lookup_dir / "L_CARRIERS.csv")

    df = load_db1b(str(db1b_path), rolling_quarters=max(0, int(args.rolling_quarters)), domestic_only=True)
    df["carrier"] = df["carrier"].astype(str).str.strip().str.upper()
    df["origin"] = df["origin"].astype(str).str.strip().str.upper()
    df["destination"] = df["destination"].astype(str).str.strip().str.upper()

    if args.carrier:
        df = df[df["carrier"] == args.carrier.strip().upper()]

    if args.year is not None:
        df = df[df["year"] == args.year]
    if args.quarter is not None:
        df = df[df["quarter"] == args.quarter]

    if df.empty:
        print("No rows after filtering.")
        return 0

    years = pd.to_numeric(df["year"], errors="coerce")
    quarters = pd.to_numeric(df["quarter"], errors="coerce")
    print(f"DB1B: {db1b_path.relative_to(base_dir) if db1b_path.is_relative_to(base_dir) else db1b_path}")
    print(f"Rows: {len(df):,}")
    print(f"Year range: {int(years.min())} - {int(years.max())}")
    print(f"Quarter values: {sorted(quarters.dropna().unique().astype(int).tolist())}")
    print(f"Carriers: {df['carrier'].nunique(dropna=True)}")
    print(f"OD pairs: {df.groupby(['origin','destination']).ngroups:,}")
    print()

    if args.carrier is None:
        carriers = _summarize_carriers(df)
        carriers = _add_lookup_columns(carriers, airport_desc={}, carrier_desc=carrier_desc)
        carriers = carriers.sort_values("passengers", ascending=False).head(args.top_n)
        print(f"Top {args.top_n} carriers by passengers:")
        cols = [c for c in ["carrier", "carrier_name", "passengers", "avg_fare", "revenue_proxy"] if c in carriers.columns]
        print(carriers[cols].to_string(index=False))
        print()

    routes = _summarize_routes(df)
    routes = routes[routes["passengers"] >= float(args.min_passengers)]
    routes = routes.sort_values(args.sort, ascending=False).head(args.top_n)
    routes = _add_lookup_columns(routes, airport_desc=airport_desc, carrier_desc={})

    print(f"Top {args.top_n} routes by {args.sort} (min_passengers={args.min_passengers:g}):")
    cols = [
        "origin",
        "origin_desc",
        "destination",
        "dest_desc",
        "passengers",
        "avg_fare",
        "avg_distance",
        "yield_per_mile",
        "revenue_proxy",
    ]
    cols = [c for c in cols if c in routes.columns]
    print(routes[cols].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
