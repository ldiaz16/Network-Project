"""
Derive airline hubs and focus cities from a BTS T-100 export.

This script reads the "T_T100_SEGMENT_ALL_CARRIER.csv" segment export (or a
normalized rollup such as data/bts_t100.csv/.parquet), aggregates seat capacity
by carrier + origin, and classifies each carrier's top origins into:
- hubs: top origins until a cumulative seat-share target is met (capped)
- focus_cities: next origins above a minimum seat-share threshold (capped)

Usage:
  python3 scripts/derive_airline_bases_from_t100.py \
    --input T_T100_SEGMENT_ALL_CARRIER.csv \
    --output resources/airline_bases_t100.json
"""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set, Tuple

sys.dont_write_bytecode = True

import pandas as pd


BASE_DIR = pathlib.Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Period:
    latest_year: int
    latest_quarter: int
    quarters: Tuple[Tuple[int, int], ...]


def _rolling_quarters(latest_year: int, latest_quarter: int, window: int) -> Tuple[Tuple[int, int], ...]:
    year = int(latest_year)
    quarter = int(latest_quarter)
    quarters: List[Tuple[int, int]] = []
    for _ in range(max(1, int(window))):
        quarters.append((year, quarter))
        quarter -= 1
        if quarter == 0:
            quarter = 4
            year -= 1
    return tuple(quarters)


_CORPORATE_SUFFIXES = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "llc",
    "ltd",
    "plc",
}


def _normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def _strip_corporate_suffix(name: str) -> str:
    parts = [p for p in re.split(r"[\s,]+", _normalize_whitespace(name)) if p]
    while parts and parts[-1].lower().strip(".") in _CORPORATE_SUFFIXES:
        parts.pop()
    return " ".join(parts)


def _derive_aliases(carrier_name: str) -> List[str]:
    """
    Generate a few stable airline-name aliases from BTS carrier labels.

    Examples:
      "Delta Air Lines Inc." -> ["Delta Air Lines Inc.", "Delta Air Lines"]
      "Breeze Aviation Group DBA  Breeze" -> ["Breeze Aviation Group DBA  Breeze", "Breeze Aviation Group", "Breeze"]
    """
    carrier_name = _normalize_whitespace(carrier_name)
    if not carrier_name:
        return []

    aliases: List[str] = [carrier_name]
    dba_match = re.search(r"\b(d/b/a|dba)\b", carrier_name, flags=re.IGNORECASE)
    if dba_match:
        before = _normalize_whitespace(carrier_name[: dba_match.start()].strip(" ,"))
        after = _normalize_whitespace(carrier_name[dba_match.end() :].strip(" ,"))
        for candidate in (before, after):
            if candidate and candidate not in aliases:
                aliases.append(candidate)

    for candidate in list(aliases):
        stripped = _strip_corporate_suffix(candidate)
        if stripped and stripped not in aliases:
            aliases.append(stripped)

    return aliases


def _detect_columns(path: pathlib.Path) -> List[str]:
    if path.suffix.lower() == ".parquet":
        # Parquet schemas can be wide; defer to reader.
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        header = handle.readline().strip()
    return [col.strip() for col in header.split(",") if col.strip()]


def _infer_schema(columns: Sequence[str]) -> str:
    cols = {c.strip() for c in columns}
    if "UNIQUE_CARRIER" in cols and "ORIGIN" in cols and "DEST" in cols:
        return "t100_segment_csv"
    lowered = {c.lower().strip() for c in cols}
    if {"carrier", "origin", "dest", "seats"}.issubset(lowered):
        return "t100_rollup"
    return "unknown"


def _read_latest_period_segment_csv(path: pathlib.Path) -> Optional[Tuple[int, int]]:
    if not path.exists():
        return None
    df = pd.read_csv(path, usecols=["YEAR", "QUARTER"])
    if df.empty:
        return None
    latest_year = int(pd.to_numeric(df["YEAR"], errors="coerce").max())
    latest_quarter = int(
        pd.to_numeric(df.loc[df["YEAR"] == latest_year, "QUARTER"], errors="coerce").max()
    )
    return latest_year, latest_quarter


def _read_latest_period_rollup(path: pathlib.Path) -> Optional[Tuple[int, int]]:
    if not path.exists():
        return None
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path, columns=["year", "quarter"])
    else:
        df = pd.read_csv(path, usecols=["year", "quarter"])
    if df.empty:
        return None
    latest_year = int(pd.to_numeric(df["year"], errors="coerce").max())
    latest_quarter = int(
        pd.to_numeric(df.loc[df["year"] == latest_year, "quarter"], errors="coerce").max()
    )
    return latest_year, latest_quarter


def _build_period(path: pathlib.Path, schema: str, rolling_quarters: int) -> Optional[Period]:
    latest = None
    if schema == "t100_segment_csv":
        latest = _read_latest_period_segment_csv(path)
    elif schema == "t100_rollup":
        latest = _read_latest_period_rollup(path)
    if not latest:
        return None
    latest_year, latest_quarter = latest
    quarters = _rolling_quarters(latest_year, latest_quarter, rolling_quarters)
    return Period(latest_year=latest_year, latest_quarter=latest_quarter, quarters=quarters)


def _aggregate_from_t100_segment_csv(
    path: pathlib.Path,
    period: Period,
    include_classes: Set[str],
) -> Tuple[Dict[str, float], Dict[Tuple[str, str], float], Dict[str, str]]:
    usecols = [
        "YEAR",
        "QUARTER",
        "CLASS",
        "UNIQUE_CARRIER",
        "UNIQUE_CARRIER_NAME",
        "CARRIER",
        "CARRIER_NAME",
        "ORIGIN",
        "DEST",
        "SEATS",
    ]
    dtype = {
        "CLASS": "string",
        "UNIQUE_CARRIER": "string",
        "UNIQUE_CARRIER_NAME": "string",
        "CARRIER": "string",
        "CARRIER_NAME": "string",
        "ORIGIN": "string",
        "DEST": "string",
    }

    quarter_set = set(period.quarters)
    quarter_keys = {year * 10 + quarter for year, quarter in quarter_set}
    carrier_totals: Dict[str, float] = defaultdict(float)
    carrier_origin_seats: Dict[Tuple[str, str], float] = defaultdict(float)
    carrier_names: Dict[str, str] = {}

    for chunk in pd.read_csv(path, usecols=usecols, dtype=dtype, chunksize=750_000):
        if chunk.empty:
            continue

        chunk["YEAR"] = pd.to_numeric(chunk["YEAR"], errors="coerce").astype("Int64")
        chunk["QUARTER"] = pd.to_numeric(chunk["QUARTER"], errors="coerce").astype("Int64")
        chunk["SEATS"] = pd.to_numeric(chunk["SEATS"], errors="coerce").fillna(0.0)

        chunk["CLASS"] = chunk["CLASS"].astype("string").str.strip().str.upper()
        chunk = chunk[chunk["CLASS"].isin(include_classes)]
        if chunk.empty:
            continue

        # Filter to rolling quarters.
        period_key = (chunk["YEAR"] * 10 + chunk["QUARTER"]).astype("Int64")
        chunk = chunk[period_key.isin(quarter_keys)]
        if chunk.empty:
            continue

        # Choose a carrier code column (prefer UNIQUE_CARRIER; fallback to CARRIER).
        carrier_series = chunk["UNIQUE_CARRIER"].fillna(chunk["CARRIER"]).astype("string")
        carrier_series = carrier_series.str.strip().str.upper()

        origin_series = chunk["ORIGIN"].astype("string").str.strip().str.upper()
        dest_series = chunk["DEST"].astype("string").str.strip().str.upper()
        seats_series = chunk["SEATS"]

        mask = (
            carrier_series.str.len() > 0
        ) & (
            origin_series.str.len() == 3
        ) & (
            dest_series.str.len() == 3
        ) & (
            origin_series != dest_series
        ) & (
            seats_series > 0
        )

        if not mask.any():
            continue

        filtered = pd.DataFrame(
            {
                "carrier": carrier_series[mask].to_numpy(),
                "origin": origin_series[mask].to_numpy(),
                "seats": seats_series[mask].to_numpy(),
            }
        )
        # Totals by carrier.
        for carrier, seats in filtered.groupby("carrier", dropna=False)["seats"].sum().items():
            carrier_totals[str(carrier)] += float(seats)

        # Origin totals.
        for (carrier, origin), seats in (
            filtered.groupby(["carrier", "origin"], dropna=False)["seats"].sum().items()
        ):
            carrier_origin_seats[(str(carrier), str(origin))] += float(seats)

        # Record carrier names (best-effort, first non-empty seen).
        name_series = chunk["UNIQUE_CARRIER_NAME"].fillna(chunk["CARRIER_NAME"]).astype("string")
        for carrier, name in zip(carrier_series[mask].to_list(), name_series[mask].to_list()):
            if not carrier or carrier in carrier_names:
                continue
            if pd.isna(name):
                continue
            cleaned = str(name).strip()
            if cleaned and cleaned.lower() not in {"nan", "<na>"}:
                carrier_names[carrier] = cleaned

    return dict(carrier_totals), dict(carrier_origin_seats), carrier_names


def _aggregate_from_rollup(
    path: pathlib.Path,
    period: Period,
) -> Tuple[Dict[str, float], Dict[Tuple[str, str], float], Dict[str, str]]:
    quarter_set = set(period.quarters)
    quarter_keys = {year * 10 + quarter for year, quarter in quarter_set}
    carrier_totals: Dict[str, float] = defaultdict(float)
    carrier_origin_seats: Dict[Tuple[str, str], float] = defaultdict(float)

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)

    df = df.rename(columns={c: c.lower().strip() for c in df.columns})
    required = {"year", "quarter", "carrier", "origin", "seats"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"Rollup file missing columns: {missing}")

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce").astype("Int64")
    df["seats"] = pd.to_numeric(df["seats"], errors="coerce").fillna(0.0)
    df["carrier"] = df["carrier"].astype("string").str.strip().str.upper()
    df["origin"] = df["origin"].astype("string").str.strip().str.upper()

    period_key = (df["year"] * 10 + df["quarter"]).astype("Int64")
    df = df[period_key.isin(quarter_keys)]
    df = df[(df["seats"] > 0) & (df["carrier"].str.len() > 0) & (df["origin"].str.len() == 3)]
    if df.empty:
        return {}, {}, {}

    for carrier, seats in df.groupby("carrier", dropna=False)["seats"].sum().items():
        carrier_totals[str(carrier)] += float(seats)
    for (carrier, origin), seats in df.groupby(["carrier", "origin"], dropna=False)["seats"].sum().items():
        carrier_origin_seats[(str(carrier), str(origin))] += float(seats)

    return dict(carrier_totals), dict(carrier_origin_seats), {}


def _derive_hubs_and_focus(
    origins: Sequence[Tuple[str, float]],
    total: float,
    *,
    hub_cumulative_target: float,
    max_hubs: int,
    focus_min_share: float,
    max_focus: int,
) -> Tuple[List[str], List[str]]:
    if total <= 0:
        return [], []

    sorted_origins = sorted(origins, key=lambda item: (-item[1], item[0]))
    shares = [(code, (seats / total) if total > 0 else 0.0) for code, seats in sorted_origins]

    hubs: List[str] = []
    cumulative = 0.0
    for code, share in shares:
        if len(hubs) >= max_hubs:
            break
        hubs.append(code)
        cumulative += float(share)
        if cumulative >= hub_cumulative_target and hubs:
            break

    focus: List[str] = []
    for code, share in shares:
        if code in hubs:
            continue
        if float(share) < focus_min_share:
            break
        focus.append(code)
        if len(focus) >= max_focus:
            break

    return hubs, focus


def build_airline_bases(
    input_path: pathlib.Path,
    *,
    rolling_quarters: int,
    include_classes: Set[str],
    hub_cumulative_target: float,
    max_hubs: int,
    focus_min_share: float,
    max_focus: int,
    min_total_seats: float,
) -> Tuple[Dict[str, dict], Optional[Period]]:
    columns = _detect_columns(input_path)
    schema = _infer_schema(columns) if columns else "t100_rollup"
    period = _build_period(input_path, schema, rolling_quarters)
    if period is None:
        raise SystemExit("Unable to determine latest YEAR/QUARTER for the provided input file.")

    if schema == "t100_segment_csv":
        totals, by_origin, names = _aggregate_from_t100_segment_csv(
            input_path,
            period=period,
            include_classes=include_classes,
        )
    elif schema == "t100_rollup":
        totals, by_origin, names = _aggregate_from_rollup(input_path, period=period)
    else:
        raise SystemExit(
            "Unrecognized input schema. Provide a BTS segment export CSV (with UNIQUE_CARRIER/ORIGIN/DEST)\n"
            "or a normalized rollup with columns: year, quarter, carrier, origin, seats."
        )

    carrier_to_origins: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for (carrier, origin), seats in by_origin.items():
        carrier_to_origins[carrier].append((origin, float(seats)))

    output: Dict[str, dict] = {}
    for carrier, origins in carrier_to_origins.items():
        total = float(totals.get(carrier) or 0.0)
        if total < min_total_seats:
            continue
        hubs, focus = _derive_hubs_and_focus(
            origins,
            total,
            hub_cumulative_target=hub_cumulative_target,
            max_hubs=max_hubs,
            focus_min_share=focus_min_share,
            max_focus=max_focus,
        )
        carrier_name = (names.get(carrier) or "").strip()
        aliases = _derive_aliases(carrier_name) if carrier_name else []
        output[carrier] = {
            "iata": carrier,
            "aliases": aliases,
            "hubs": hubs,
            "focus_cities": focus,
            "off_points": [],
            "meta": {
                "metric": "seats",
                "latest_year": period.latest_year,
                "latest_quarter": period.latest_quarter,
                "rolling_quarters": rolling_quarters,
            },
        }

    return output, period


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Derive hubs and focus cities from BTS T-100 data.")
    parser.add_argument(
        "--input",
        default=str(BASE_DIR / "T_T100_SEGMENT_ALL_CARRIER.csv"),
        help="Path to T_T100_SEGMENT_ALL_CARRIER.csv or a normalized T-100 rollup (csv/parquet).",
    )
    parser.add_argument(
        "--output",
        default=str(BASE_DIR / "resources" / "airline_bases_t100.json"),
        help="Where to write the derived bases JSON.",
    )
    parser.add_argument("--rolling-quarters", type=int, default=4, help="How many trailing quarters to include.")
    parser.add_argument(
        "--include-classes",
        default="F,L",
        help="Comma-separated CLASS codes to include for segment exports (default: F,L).",
    )
    parser.add_argument(
        "--hub-cumulative-target",
        type=float,
        default=0.60,
        help="Cumulative seat share to capture in hubs before stopping (capped by --max-hubs).",
    )
    parser.add_argument("--max-hubs", type=int, default=10, help="Maximum hubs to emit per carrier.")
    parser.add_argument(
        "--focus-min-share",
        type=float,
        default=0.003,
        help="Minimum seat share for a non-hub origin to be included as a focus city.",
    )
    parser.add_argument("--max-focus", type=int, default=10, help="Maximum focus cities to emit per carrier.")
    parser.add_argument(
        "--min-total-seats",
        type=float,
        default=1_000_000.0,
        help="Skip carriers with fewer total seats than this threshold in the selected period.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    input_path = pathlib.Path(args.input).expanduser().resolve()
    output_path = pathlib.Path(args.output).expanduser().resolve()

    include_classes = {c.strip().upper() for c in str(args.include_classes).split(",") if c.strip()}
    if not include_classes:
        include_classes = {"F", "L"}

    bases, period = build_airline_bases(
        input_path,
        rolling_quarters=int(args.rolling_quarters),
        include_classes=include_classes,
        hub_cumulative_target=float(args.hub_cumulative_target),
        max_hubs=int(args.max_hubs),
        focus_min_share=float(args.focus_min_share),
        max_focus=int(args.max_focus),
        min_total_seats=float(args.min_total_seats),
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(bases, handle, indent=2, sort_keys=True)
        handle.write("\n")

    carrier_count = len(bases)
    latest_desc = f"{period.latest_year}Q{period.latest_quarter}" if period else "unknown"
    print(f"Wrote {carrier_count} carriers to {output_path} (latest={latest_desc}, classes={sorted(include_classes)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
