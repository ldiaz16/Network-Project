#!/usr/bin/env python3
"""
Phase 2 — Market ranking & stability analysis (Domestic Demand Mart).

Outputs (default `reports/`):
  - domestic_market_top50.csv
  - domestic_market_top50_excl_nyc_la_chi.csv
  - domestic_market_concentration.md
  - domestic_market_stability.csv
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

from src.demand_analysis import (
    BIG3_AIRPORTS,
    compute_concentration,
    aggregate_market_totals,
    build_market_quarterly,
    load_domestic_demand_mart,
    market_stability_analysis,
    rank_markets,
)


def _default_mart_path(base_dir: Path) -> Path:
    candidates = [
        base_dir / "datasets" / "db1b" / "processed" / "domestic_demand_mart.parquet",
        base_dir / "datasets" / "db1b" / "processed" / "domestic_demand_mart.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main() -> int:
    base_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Market ranking & stability from Domestic Demand Mart.")
    parser.add_argument("--mart-path", type=Path, default=_default_mart_path(base_dir))
    parser.add_argument("--since-year", type=int, default=2022)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--out-dir", type=Path, default=base_dir / "reports")
    parser.add_argument("--directional", action="store_true", help="Treat markets as directional (origin->dest). Default is undirected.")
    args = parser.parse_args()

    mart_path: Path = args.mart_path
    if not mart_path.exists():
        raise SystemExit(f"Missing demand mart: {mart_path}")

    mart = load_domestic_demand_mart(mart_path)
    market_quarterly = build_market_quarterly(mart, directional=bool(args.directional), since_year=int(args.since_year))
    market_totals = aggregate_market_totals(market_quarterly)

    top_all = rank_markets(market_totals, top_n=int(args.top_n))
    top_excl = rank_markets(market_totals, top_n=int(args.top_n), exclude_airports=BIG3_AIRPORTS)

    out_dir: Path = args.out_dir
    _write_csv(top_all, out_dir / "domestic_market_top50.csv")
    _write_csv(top_excl, out_dir / "domestic_market_top50_excl_nyc_la_chi.csv")

    concentration = compute_concentration(market_totals, top_share=0.10)
    top_1pct = compute_concentration(market_totals, top_share=0.01)

    concentration_md = (
        "# Domestic demand concentration\n\n"
        f"- Markets: {concentration.markets:,}\n"
        f"- Total passengers (since {args.since_year}): {concentration.total_passengers:,.0f}\n"
        f"- Top 10% markets: {concentration.top_markets:,} ({concentration.top_passenger_share:.1%} of passengers)\n"
        f"- Long tail 90%: {concentration.long_tail_markets:,} ({concentration.long_tail_passenger_share:.1%} of passengers)\n"
        f"- Top 1% markets: {top_1pct.top_markets:,} ({top_1pct.top_passenger_share:.1%} of passengers)\n"
        "\n"
        "Notes:\n"
        "- Market definition is undirected airport-pair unless `--directional` is set.\n"
        "- Passengers are summed across all quarters in the mart window.\n"
    )
    _write_markdown(out_dir / "domestic_market_concentration.md", concentration_md)

    stability = market_stability_analysis(market_quarterly)
    _write_csv(stability, out_dir / "domestic_market_stability.csv")

    class_counts = stability["classification"].value_counts(dropna=False).to_dict() if "classification" in stability.columns else {}
    print(f"Demand mart: {mart_path.relative_to(base_dir) if mart_path.is_relative_to(base_dir) else mart_path}")
    print(f"Window: {args.since_year}+; quarters={market_quarterly[['year','quarter']].drop_duplicates().shape[0]}")
    print(f"Markets: {market_totals.shape[0]:,} (directional={bool(args.directional)})")
    print(f"Top 10% share: {concentration.top_passenger_share:.1%}")
    print("Stability classes:", ", ".join(f"{k}={v:,}" for k, v in class_counts.items()))
    print(f"Wrote: {out_dir / 'domestic_market_top50.csv'}")
    print(f"Wrote: {out_dir / 'domestic_market_top50_excl_nyc_la_chi.csv'}")
    print(f"Wrote: {out_dir / 'domestic_market_concentration.md'}")
    print(f"Wrote: {out_dir / 'domestic_market_stability.csv'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
