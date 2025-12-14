"""
Convert BTS .asc dumps (DB1B public, T-100 segment) into Parquet files the app can ingest.

Input folder: "BTS DATA" in repo root (pipe-delimited, no headers).
Outputs:
  data/db1b.parquet   -> DB1B market-level sample (carrier, origin, dest, passengers, fare, year, quarter)
  data/bts_t100.parquet -> T-100 segments (carrier, origin, dest, seats, passengers, departures, distance, year, quarter)

Notes:
- Streaming parser to handle large files; two passes per file type (pass1 to find latest quarter/month, pass2 to filter rolling 4Q).
- Schema assumptions follow BTS standard layouts; if BTS publishes a new layout, adjust column maps below.
"""

import csv
import pathlib
from typing import Iterable, List, Optional, Tuple

import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "BTS DATA"
OUT_DB1B = BASE_DIR / "data" / "db1b.parquet"
OUT_T100 = BASE_DIR / "data" / "bts_t100.parquet"


def iter_rows(path: pathlib.Path) -> Iterable[List[str]]:
    with path.open("r", newline="") as fh:
        reader = csv.reader(fh, delimiter="|")
        for row in reader:
            yield row


def find_latest_db1b_quarter(files: List[pathlib.Path]) -> Optional[Tuple[int, int]]:
    latest: Optional[Tuple[int, int]] = None
    for path in files:
        for row in iter_rows(path):
            if len(row) < 4:
                continue
            try:
                yearqtr = int(row[2])
                quarter = int(row[3])
                year = int(str(yearqtr)[:4])
            except Exception:
                continue
            candidate = (year, quarter)
            if latest is None or candidate > latest:
                latest = candidate
    return latest


def rolling_quarters(latest: Tuple[int, int], window: int = 4) -> List[Tuple[int, int]]:
    year, q = latest
    out = []
    for _ in range(window):
        out.append((year, q))
        q -= 1
        if q == 0:
            q = 4
            year -= 1
    return out


def convert_db1b(files: List[pathlib.Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame(columns=["year", "quarter", "carrier", "origin", "dest", "passengers", "fare"])

    latest = find_latest_db1b_quarter(files)
    if not latest:
        return pd.DataFrame(columns=["year", "quarter", "carrier", "origin", "dest", "passengers", "fare"])
    targets = set(rolling_quarters(latest, 4))

    records = []
    for path in files:
        for row in iter_rows(path):
            if len(row) < 16:
                continue
            try:
                yearqtr = int(row[2])
                quarter = int(row[3])
                year = int(str(yearqtr)[:4])
            except Exception:
                continue
            if (year, quarter) not in targets:
                continue
            # Treat db1b.public.* as DB28 Market layout if present: indexes per market PDF
            origin = row[2].strip() if len(row) > 2 else ""
            dest = row[6].strip() if len(row) > 6 else ""
            carrier = row[10].strip() if len(row) > 10 and row[10].strip() else (row[11].strip() if len(row) > 11 else "")
            if not (origin and dest and carrier):
                continue
            try:
                fare = float(row[14]) if len(row) > 14 and row[14] else 0.0
            except Exception:
                fare = 0.0
            try:
                pax = float(row[15]) if len(row) > 15 and row[15] else 0.0
            except Exception:
                pax = 0.0
            records.append(
                {
                    "year": year,
                    "quarter": quarter,
                    "carrier": carrier,
                    "origin": origin,
                    "dest": dest,
                    "passengers": pax,
                    "fare": fare,
                }
            )
    return pd.DataFrame.from_records(records)


def find_latest_t100_month(files: List[pathlib.Path]) -> Optional[Tuple[int, int]]:
    latest: Optional[Tuple[int, int]] = None
    for path in files:
        for row in iter_rows(path):
            if len(row) < 2:
                continue
            try:
                year = int(row[0])
                month = int(row[1])
            except Exception:
                continue
            candidate = (year, month)
            if latest is None or candidate > latest:
                latest = candidate
    return latest


def month_to_quarter(month: int) -> int:
    if month <= 0:
        return 1
    return (month - 1) // 3 + 1


def convert_t100(files: List[pathlib.Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame(columns=["year", "quarter", "carrier", "origin", "dest", "departures", "seats", "passengers", "distance"])

    latest = find_latest_t100_month(files)
    if not latest:
        return pd.DataFrame(columns=["year", "quarter", "carrier", "origin", "dest", "departures", "seats", "passengers", "distance"])

    latest_year, latest_month = latest
    latest_quarter = month_to_quarter(latest_month)
    targets = set(rolling_quarters((latest_year, latest_quarter), 4))

    records = []
    # DB28 Segment field positions per reference PDF (post-2020):
    # 0 Year, 1 Month, 2 Origin Alpha, 3 Origin ID, 4 Origin WAC, 5 Origin City,
    # 6 Dest Alpha, 7 Dest ID, 8 Dest WAC, 9 Dest City,
    # 10 Carrier Alpha, 11 Carrier Entity, 12 Group Code,
    # 13 Distance, 14 Service Class, 15 Aircraft Group, 16 Aircraft Type, 17 Aircraft Config,
    # 18 Departures Performed, 19 Departures Scheduled, 20 Payload,
    # 21 Seats, 22 Middle Cabin (0), 23 Coach Cabin (0),
    # 24 Passengers, 25 Middle Pax (0), 26 Coach Pax (0),
    # 27 Freight, 28 Mail, 29 Ramp Minutes, 30 Air Minutes, 31 Carrier WAC.
    for path in files:
        for row in iter_rows(path):
            if len(row) < 25:  # need passengers at index 24
                continue
            try:
                year = int(row[0])
                month = int(row[1])
            except Exception:
                continue
            quarter = month_to_quarter(month)
            if (year, quarter) not in targets:
                continue
            origin = row[2].strip() if len(row) > 2 else ""
            dest = row[6].strip() if len(row) > 6 else ""
            carrier = row[10].strip() if len(row) > 10 else ""
            if not (origin and dest and carrier):
                continue
            def _to_float(idx):
                try:
                    return float(row[idx]) if len(row) > idx and row[idx] else 0.0
                except Exception:
                    return 0.0
            departures = _to_float(18)
            seats = _to_float(21)
            pax = _to_float(24)
            distance = _to_float(13)
            carrier_wac = row[31].strip() if len(row) > 31 and row[31] else None
            records.append(
                {
                    "year": year,
                    "quarter": quarter,
                    "carrier": carrier,
                    "origin": origin,
                    "dest": dest,
                    "departures": departures,
                    "seats": seats,
                    "passengers": pax,
                    "distance": distance,
                    "carrier_wac": carrier_wac,
                }
            )
    return pd.DataFrame.from_records(records)


def main():
    if not RAW_DIR.exists():
        raise SystemExit(f"Missing raw directory: {RAW_DIR}")

    db1b_files = sorted(RAW_DIR.glob("db1b.public.*.asc"))
    t100_files = sorted(RAW_DIR.glob("dd.db28ds.*.asc"))

    def _latest(files: List[pathlib.Path], n: int) -> List[pathlib.Path]:
        def _key(path: pathlib.Path):
            digits = "".join(ch for ch in path.stem if ch.isdigit())
            try:
                return int(digits)
            except Exception:
                return 0
        return sorted(files, key=_key)[-n:] if files else []

    db1b_files = _latest(db1b_files, 4)
    t100_files = _latest(t100_files, 2)

    print(f"Found {len(db1b_files)} DB1B files, {len(t100_files)} T-100 segment files.")

    db_df = convert_db1b(db1b_files)
    t100_df = convert_t100(t100_files)

    OUT_DB1B.parent.mkdir(parents=True, exist_ok=True)
    try:
        db_df.to_parquet(OUT_DB1B, index=False)
    except Exception:
        OUT_DB1B.with_suffix(".csv").write_text("")  # placeholder creation
        OUT_DB1B_csv = OUT_DB1B.with_suffix(".csv")
        db_df.to_csv(OUT_DB1B_csv, index=False)
        print(f"Parquet not available; wrote CSV instead: {OUT_DB1B_csv}")
    else:
        print(f"Wrote {len(db_df)} DB1B rows to {OUT_DB1B}")

    try:
        t100_df.to_parquet(OUT_T100, index=False)
    except Exception:
        OUT_T100_csv = OUT_T100.with_suffix(".csv")
        t100_df.to_csv(OUT_T100_csv, index=False)
        print(f"Parquet not available; wrote CSV instead: {OUT_T100_csv}")
    else:
        print(f"Wrote {len(t100_df)} T-100 rows to {OUT_T100}")


if __name__ == "__main__":
    main()
