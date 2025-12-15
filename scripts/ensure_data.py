#!/usr/bin/env python3
"""
Simple helper to guard required CSV inputs for the airline optimizer.
"""

from pathlib import Path


def main() -> None:
    required = [
        Path("Lookup Tables/L_UNIQUE_CARRIERS.csv"),
        Path("Lookup Tables/L_AIRPORT.csv"),
        Path("data/cbsa.csv"),
    ]
    missing = [str(path) for path in required if not path.exists()]

    t100_csv = Path("T_T100_SEGMENT_ALL_CARRIER.csv")
    t100_gz = Path("T_T100_SEGMENT_ALL_CARRIER.csv.gz")
    if not (t100_csv.exists() or t100_gz.exists()):
        missing.append("T_T100_SEGMENT_ALL_CARRIER.csv(.gz)")
    if missing:
        raise SystemExit(
            "Missing required data files:\n- " + "\n- ".join(missing)
        )
    print("Data check: ok")


if __name__ == "__main__":
    main()
