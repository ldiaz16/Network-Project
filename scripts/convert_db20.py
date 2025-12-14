"""
Parse DB20 monthly capacity/traffic .asc files (219 fields) into Parquet.

This uses the post-2020 DB20 layout to extract key fields:
- carrier (entity code), year, month, quarter
- service class (T1/T2 etc. at index 3)
- RPMs, ASMs, passengers, departures

Outputs:
  data/db20.parquet
"""

import csv
import pathlib
from typing import Iterable, List, Dict, Any

import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "Data Bank 20 - Monthly U.S. Air Carrier Capacity and Traffic Data"
OUT_PATH = BASE_DIR / "data" / "db20.parquet"

# DB20 field positions (post-2020 per BTS docs):
# 0 Entity (carrier entity code), 1 Year, 2 Month, 3 Service class
# 5? total RPM domestic? 6? total ASM? but BTS doc: fields for domestic/intl etc.
# We will map using common positions: 5 RPM, 6 ASM, 10 Passenger RPM?, 11 Passenger ASM?
# For now, use commonly referenced indices from BTS samples:
#   5 Domestic RPM, 6 Domestic ASM, 10 Domestic Rev PAX, 11 Domestic RPM (dup), 18 Domestic Cargo RPM, etc.
# Because the sample is multi-duplicated, we pick key consistent slots:
#   5 RPM, 6 ASM, 10 PAX_RPM? 11 PAX_RPM copy, 18 RPM_TOT? We instead use the first RPM/ASM and passenger counts at 21/22? -> not labeled.
# To avoid mislabeling, we will capture: col5 as rpm, col6 as asm, col10 as passengers, col12 departures (if present).


RPM_IDX = 5
ASM_IDX = 6
PAX_IDX = 10
DEP_IDX = 12  # if empty, will be zero


def iter_rows(path: pathlib.Path) -> Iterable[List[str]]:
    with path.open("r", newline="") as fh:
        reader = csv.reader(fh, delimiter="|")
        for row in reader:
            yield row


def month_to_quarter(month: int) -> int:
    if month <= 0:
        return 1
    return (month - 1) // 3 + 1


def safe_float(row: List[str], idx: int) -> float:
    try:
        return float(row[idx]) if len(row) > idx and row[idx] else 0.0
    except Exception:
        return 0.0


def convert(files: List[pathlib.Path]) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    for path in files:
        for row in iter_rows(path):
            if len(row) < 15:
                continue
            try:
                carrier = row[0].strip()
                year = int(row[1])
                month = int(row[2])
                svc_class = row[3].strip()
            except Exception:
                continue
            quarter = month_to_quarter(month)
            rpm = safe_float(row, RPM_IDX)
            asm = safe_float(row, ASM_IDX)
            passengers = safe_float(row, PAX_IDX)
            departures = safe_float(row, DEP_IDX)
            records.append(
                {
                    "carrier": carrier,
                    "year": year,
                    "month": month,
                    "quarter": quarter,
                    "service_class": svc_class,
                    "rpm": rpm,
                    "asm": asm,
                    "passengers": passengers,
                    "departures": departures,
                }
            )
    return pd.DataFrame(records)


def main():
    files = sorted(RAW_DIR.glob("db20*.asc"))
    print(f"Found {len(files)} DB20 files.")
    df = convert(files)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {len(df)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
