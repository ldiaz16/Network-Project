"""
Convert DB28 Segment and Market .asc files (pipe-delimited, no headers) to Parquet.

This uses the official DB28 record layouts (post-2020 ref PDFs), which also align with
pre-2020 field order for the core fields we extract.

Outputs:
  data/db28_segment.parquet  (year, month, quarter, carrier, origin, dest, distance, seats, passengers, freight, mail, carrier_wac)
  data/db28_market.parquet   (year, month, quarter, carrier, origin, dest, distance, passengers, freight, mail, carrier_wac)

Usage:
  python3 scripts/convert_db28.py
"""

import csv
import pathlib
from typing import Iterable, List, Tuple

import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "BTS DATA"
OUT_SEG = BASE_DIR / "data" / "db28_segment.parquet"
OUT_MKT = BASE_DIR / "data" / "db28_market.parquet"


def iter_rows(path: pathlib.Path) -> Iterable[List[str]]:
    with path.open("r", newline="") as fh:
        reader = csv.reader(fh, delimiter="|")
        for row in reader:
            yield row


def month_to_quarter(month: int) -> int:
    if month <= 0:
        return 1
    return (month - 1) // 3 + 1


def rolling_quarters_from_latest(latest: Tuple[int, int], window: int = 12) -> set:
    # window in quarters; for pre-COVID analysis we will later filter year ranges explicitly.
    y, q = latest
    out = []
    for _ in range(window):
        out.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    return set(out)


def find_latest(files: List[pathlib.Path], year_idx: int, month_idx: int) -> Tuple[int, int]:
    latest = (0, 0)
    for path in files:
        for row in iter_rows(path):
            if len(row) <= max(year_idx, month_idx):
                continue
            try:
                year = int(row[year_idx])
                month = int(row[month_idx])
            except Exception:
                continue
            quarter = month_to_quarter(month)
            if (year, quarter) > latest:
                latest = (year, quarter)
    return latest


def convert_segment(files: List[pathlib.Path], year_filter: Tuple[int, int] = None) -> pd.DataFrame:
    """
    DB28 Segment field positions (0-based):
      0 Year, 1 Month, 2 Origin alpha, 3 Origin ID, 4 Origin WAC, 6 Dest alpha, 7 Dest ID, 8 Dest WAC,
      10 Carrier alpha, 11 Carrier entity, 13 Distance, 14 Service class,
      21 Seats (available), 24 Passengers transported, 27 Freight, 28 Mail, 31 Carrier WAC.
    """
    rows = []
    latest = find_latest(files, 0, 1)
    targets = rolling_quarters_from_latest(latest, 16)  # 4 years window
    for path in files:
        for row in iter_rows(path):
            if len(row) < 25:
                continue
            try:
                year = int(row[0]); month = int(row[1])
            except Exception:
                continue
            quarter = month_to_quarter(month)
            if (year, quarter) not in targets:
                continue
            if year_filter and not (year_filter[0] <= year <= year_filter[1]):
                continue
            origin = row[2].strip() if len(row) > 2 else ""
            dest = row[6].strip() if len(row) > 6 else ""
            carrier = row[10].strip() if len(row) > 10 else ""
            if not (origin and dest and carrier):
                continue
            def f(idx):
                try:
                    return float(row[idx]) if len(row) > idx and row[idx] else 0.0
                except Exception:
                    return 0.0
            distance = f(13)
            seats = f(21)
            pax = f(24)
            freight = f(27)
            mail = f(28)
            carrier_wac = row[31].strip() if len(row) > 31 and row[31] else None
            rows.append({
                "year": year,
                "month": month,
                "quarter": quarter,
                "carrier": carrier,
                "origin": origin,
                "dest": dest,
                "distance": distance,
                "seats": seats,
                "passengers": pax,
                "freight": freight,
                "mail": mail,
                "carrier_wac": carrier_wac,
            })
    return pd.DataFrame(rows)


def convert_market(files: List[pathlib.Path], year_filter: Tuple[int, int] = None) -> pd.DataFrame:
    """
    DB28 Market field positions (0-based):
      0 Year, 1 Month, 2 Origin alpha, 3 Origin ID, 4 Origin WAC,
      6 Dest alpha, 7 Dest ID, 8 Dest WAC,
      10 Carrier alpha, 11 Carrier entity, 13 Group (skip), 14 Distance, 15 Service class,
      15? per PDF passengers at field 16 => index 15, freight 18 => index 18, mail 19 => index 19,
      carrier WAC at field 19? actually field 19 in market doc is freight, carrier WAC at last field (index 19 or 20 depending); use index 19+ if exists.
    """
    rows = []
    latest = find_latest(files, 0, 1)
    targets = rolling_quarters_from_latest(latest, 16)
    for path in files:
        for row in iter_rows(path):
            if len(row) < 16:
                continue
            try:
                year = int(row[0]); month = int(row[1])
            except Exception:
                continue
            quarter = month_to_quarter(month)
            if (year, quarter) not in targets:
                continue
            if year_filter and not (year_filter[0] <= year <= year_filter[1]):
                continue
            origin = row[2].strip() if len(row) > 2 else ""
            dest = row[6].strip() if len(row) > 6 else ""
            carrier = row[10].strip() if len(row) > 10 else ""
            if not (origin and dest and carrier):
                continue
            def f(idx):
                try:
                    return float(row[idx]) if len(row) > idx and row[idx] else 0.0
                except Exception:
                    return 0.0
            distance = f(14)
            pax = f(15)  # passengers enplaned
            freight = f(18) if len(row) > 18 else 0.0
            mail = f(19) if len(row) > 19 else 0.0
            carrier_wac = row[20].strip() if len(row) > 20 and row[20] else (row[-1].strip() if row else None)
            rows.append({
                "year": year,
                "month": month,
                "quarter": quarter,
                "carrier": carrier,
                "origin": origin,
                "dest": dest,
                "distance": distance,
                "passengers": pax,
                "freight": freight,
                "mail": mail,
                "carrier_wac": carrier_wac,
            })
    return pd.DataFrame(rows)


def main():
    seg_files = sorted(RAW_DIR.glob("dd.db28ds*.asc")) + sorted(RAW_DIR.glob("db28seg*.asc"))
    mkt_files = sorted(RAW_DIR.glob("dd.db28dm*.asc")) + sorted(RAW_DIR.glob("db28mkt*.asc"))

    print(f"Found {len(seg_files)} segment files, {len(mkt_files)} market files.")
    # Training window pre-COVID
    year_window = (2017, 2019)
    seg_df = convert_segment(seg_files, year_filter=year_window)
    mkt_df = convert_market(mkt_files, year_filter=year_window)

    OUT_SEG.parent.mkdir(parents=True, exist_ok=True)
    seg_df.to_parquet(OUT_SEG, index=False)
    mkt_df.to_parquet(OUT_MKT, index=False)
    print(f"Wrote segments: {len(seg_df)} -> {OUT_SEG}")
    print(f"Wrote markets : {len(mkt_df)} -> {OUT_MKT}")


if __name__ == "__main__":
    main()
