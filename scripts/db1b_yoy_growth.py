#!/usr/bin/env python3
"""
List carriers sorted by year-over-year growth between the earliest available DB1B year and the latest.
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


def _format_num(num: float) -> str:
    if pd.isna(num):
        return "n/a"
    return f"{int(round(num)):,.0f}"


def _format_pct(value: float | pd.NA) -> str:
    if value is pd.NA or pd.isna(value):
        return "n/a"
    return f"{value:,.1f}%"


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [max(len(headers[i]), *(len(row[i]) for row in rows)) for i in range(len(headers))]
    hline = "-+-".join("-" * w for w in widths)
    header_row = " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers)))
    body = "\n".join(
        " | ".join(rows[j][i].ljust(widths[i]) for i in range(len(headers)))
        for j in range(len(rows))
    )
    return f"{header_row}\n{hline}\n{body}"


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Rank carriers by passenger growth from the first to last DB1B year.")
    parser.add_argument("--db1b-path", type=Path, default=_default_db1b_path(base_dir))
    parser.add_argument("--top-n", type=int, default=15)
    parser.add_argument(
        "--min-earliest-passengers",
        type=float,
        default=10000.0,
        help="Minimum passengers in the earliest year so very tiny carriers are skipped.",
    )
    parser.add_argument(
        "--min-latest-passengers",
        type=float,
        default=1000.0,
        help="Minimum passengers in the latest year (default 1k).",
    )
    args = parser.parse_args()

    db1b_path: Path = args.db1b_path
    if not db1b_path.exists():
        raise SystemExit(f"Missing DB1B dataset: {db1b_path}")

    df = pd.read_parquet(db1b_path, columns=["year", "carrier", "passengers"])
    df["carrier"] = df["carrier"].astype(str).str.strip().str.upper()
    df = df[df["passengers"] > 0]

    grouped = df.groupby(["carrier", "year"], as_index=False, sort=True)["passengers"].sum()
    years = sorted(grouped["year"].unique().tolist())
    if len(years) < 2:
        raise SystemExit("Need at least two years of data to compute growth.")

    earliest_year, latest_year = years[0], years[-1]
    earliest = grouped[grouped["year"] == earliest_year][["carrier", "passengers"]].rename(
        columns={"passengers": "passengers_earliest"}
    )
    latest = grouped[grouped["year"] == latest_year][["carrier", "passengers"]].rename(
        columns={"passengers": "passengers_latest"}
    )

    growth = earliest.merge(latest, on="carrier", how="outer").fillna(0.0)
    growth["abs_growth"] = growth["passengers_latest"] - growth["passengers_earliest"]
    mask = growth["passengers_earliest"] > 0
    growth.loc[mask, "pct_growth"] = growth.loc[mask, "abs_growth"] / growth.loc[mask, "passengers_earliest"] * 100
    growth.loc[~mask, "pct_growth"] = pd.NA

    growth = growth[
        (growth["passengers_earliest"] >= args.min_earliest_passengers)
        & (growth["passengers_latest"] >= args.min_latest_passengers)
    ]
    if growth.empty:
        raise SystemExit(
            "No carriers remain after applying the passenger thresholds; lower the minimums to see data."
        )

    growth = growth.sort_values("pct_growth", ascending=False)
    display = growth.head(args.top_n)

    print(
        f"DB1B YoY growth from {earliest_year} -> {latest_year} "
        f"(earliest ≥ {args.min_earliest_passengers:,.0f}, latest ≥ {args.min_latest_passengers:,.0f} passengers)"
    )
    print()
    table = display.assign(
        passengers_earliest=lambda df: df["passengers_earliest"].map(_format_num),
        passengers_latest=lambda df: df["passengers_latest"].map(_format_num),
        abs_growth=lambda df: df["abs_growth"].map(lambda n: f"{n:,.0f}"),
        pct_growth=lambda df: df["pct_growth"].map(_format_pct),
    )
    cols = [
        "carrier",
        "passengers_earliest",
        "passengers_latest",
        "abs_growth",
        "pct_growth",
    ]
    rows = table[cols].astype(str).values.tolist()
    print(_render_table(cols, rows))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
