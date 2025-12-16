#!/usr/bin/env python3
"""
Convert TranStats DB1B Market ZIP/CSV exports into Parquet files the project can load.

Input: a directory containing TranStats DB1BMarket ZIPs and/or extracted CSVs named like:
  Origin_and_Destination_Survey_DB1BMarket_2024_2.zip

Outputs (by default, aggregated to carrier+OD+quarter to keep files small):
  - datasets/db1b/processed/market_parquet/db1b_market_<year>_q<quarter>.parquet  (per quarter)
  - datasets/db1b/processed/db1b.parquet                                         (combined)

The combined Parquet is consumed by `src/load_data.py` if present.

Examples:
  - Convert all quarters (domestic only):
      python3 scripts/convert_db1b_market_zips.py --domestic-only
  - Keep only O&Ds touching New York:
      python3 scripts/convert_db1b_market_zips.py --domestic-only --filter-state NY
"""

from __future__ import annotations

import argparse
import re
import shutil
import zipfile
from pathlib import Path


READ_CSV_CALL_SQL = """
read_csv(
    ?,
    delim=',',
    header=true,
    quote='"',
    escape='"',
    ignore_errors=true,
    null_padding=true,
    strict_mode=false,
    max_line_size=10000000
)
""".strip()


def _sql_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _parse_year_quarter(path: Path) -> tuple[int | None, int | None]:
    name = path.name
    patterns = [
        r"Origin_and_Destination_Survey_DB1BMarket_(\d{4})_(\d)\.zip$",
        r"Origin_and_Destination_Survey_DB1BMarket_(\d{4})_(\d)\.csv$",
        r"DB1BMarket_(\d{4})_Q(\d)\.zip$",
        r"DB1BMarket_(\d{4})_Q(\d)\.csv$",
    ]
    for pat in patterns:
        match = re.match(pat, name, flags=re.IGNORECASE)
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None


def _find_single_csv_in_zip(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n and not n.endswith("/")]
    candidates: list[str] = []
    for name in names:
        lower = name.lower()
        if lower.endswith("readme.html"):
            continue
        if lower.startswith("__macosx/") or Path(name).name.startswith("._"):
            continue
        if lower.endswith(".csv"):
            candidates.append(name)
    if len(candidates) != 1:
        raise ValueError(f"Expected 1 CSV inside {zip_path.name}, found {len(candidates)}")
    return candidates[0]


def _extract_csv(zip_path: Path, *, extract_dir: Path) -> Path:
    extract_dir.mkdir(parents=True, exist_ok=True)
    inner_csv = _find_single_csv_in_zip(zip_path)
    extracted_path = extract_dir / Path(inner_csv).name
    if extracted_path.exists():
        extracted_path.unlink()
    with zipfile.ZipFile(zip_path) as zf, zf.open(inner_csv) as src, extracted_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    return extracted_path


def _convert_one_csv(
    con,
    csv_path: Path,
    *,
    out_path: Path,
    domestic_only: bool,
    filter_state: str | None,
    aggregate: bool,
) -> int:
    where = [
        f"{_sql_ident('Origin')} IS NOT NULL AND {_sql_ident('Origin')} <> ''",
        f"{_sql_ident('Dest')} IS NOT NULL AND {_sql_ident('Dest')} <> ''",
        f"{_sql_ident('RPCarrier')} IS NOT NULL AND {_sql_ident('RPCarrier')} <> ''",
    ]
    if domestic_only:
        where.append(
            f"{_sql_ident('OriginCountry')} = 'US' AND {_sql_ident('DestCountry')} = 'US'"
        )
    if filter_state:
        state = filter_state.strip().upper()
        where.append(
            f"({_sql_ident('OriginState')} = '{state}' OR {_sql_ident('DestState')} = '{state}')"
        )
    where_clause = "WHERE " + " AND ".join(where)

    raw_sql = f"""
        SELECT
            CAST({_sql_ident('Year')} AS INTEGER) AS year,
            CAST({_sql_ident('Quarter')} AS INTEGER) AS quarter,
            CAST({_sql_ident('RPCarrier')} AS VARCHAR) AS carrier,
            CAST({_sql_ident('Origin')} AS VARCHAR) AS origin,
            CAST({_sql_ident('Dest')} AS VARCHAR) AS destination,
            CAST({_sql_ident('Passengers')} AS DOUBLE) AS passengers,
            CAST({_sql_ident('MktFare')} AS DOUBLE) AS fare,
            CAST({_sql_ident('MktMilesFlown')} AS DOUBLE) AS distance
        FROM {READ_CSV_CALL_SQL}
        {where_clause}
    """.strip()

    if aggregate:
        select_sql = f"""
            WITH raw AS (
                {raw_sql}
            )
            SELECT
                year,
                quarter,
                carrier,
                origin,
                destination,
                SUM(passengers) AS passengers,
                CASE
                    WHEN SUM(passengers) > 0 THEN SUM(fare * passengers) / SUM(passengers)
                    ELSE 0
                END AS fare,
                CASE
                    WHEN SUM(passengers) > 0 THEN SUM(distance * passengers) / SUM(passengers)
                    ELSE 0
                END AS distance
            FROM raw
            GROUP BY year, quarter, carrier, origin, destination
        """.strip()
    else:
        select_sql = raw_sql

    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    out_literal = str(out_path).replace("'", "''")
    copy_sql = f"COPY ({select_sql}) TO '{out_literal}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    con.execute(copy_sql, [str(csv_path)])

    rows = int(con.execute("SELECT COUNT(*) FROM parquet_scan(?)", [str(out_path)]).fetchone()[0])
    return rows


def _convert_one_zip(
    con,
    zip_path: Path,
    *,
    out_path: Path,
    extract_dir: Path,
    domestic_only: bool,
    filter_state: str | None,
    aggregate: bool,
) -> int:
    csv_path = _extract_csv(zip_path, extract_dir=extract_dir)
    try:
        return _convert_one_csv(
            con,
            csv_path,
            out_path=out_path,
            domestic_only=domestic_only,
            filter_state=filter_state,
            aggregate=aggregate,
        )
    finally:
        try:
            csv_path.unlink()
        except OSError:
            pass


def _is_db1b_market_path(path: Path) -> bool:
    if not path.is_file():
        return False
    name = path.name.lower()
    if "db1bmarket" not in name:
        return False
    return path.suffix.lower() in {".zip", ".csv"}


def _collect_inputs(input_dir: Path) -> list[Path]:
    if not input_dir.exists():
        return []
    candidates = [p for p in input_dir.rglob("*") if _is_db1b_market_path(p)]
    return sorted(candidates, key=lambda p: p.name)


def _dedupe_inputs(inputs: list[Path]) -> list[Path]:
    """Prefer ZIP over CSV when both exist for the same year/quarter."""
    chosen: dict[tuple[int, int], Path] = {}
    unknown: list[Path] = []

    def _score(path: Path) -> tuple[int, str]:
        return (1 if path.suffix.lower() == ".zip" else 0, path.name.lower())

    for path in inputs:
        year, quarter = _parse_year_quarter(path)
        if not year or not quarter:
            unknown.append(path)
            continue
        key = (year, quarter)
        existing = chosen.get(key)
        if existing is None or _score(path) > _score(existing):
            chosen[key] = path

    deduped = sorted(chosen.values(), key=lambda p: (_parse_year_quarter(p)[0] or 0, _parse_year_quarter(p)[1] or 0, p.name))
    deduped.extend(sorted(unknown, key=lambda p: p.name))
    return deduped


def _default_input_dir(base_dir: Path) -> Path:
    candidates = [
        base_dir / "datasets" / "db1b" / "raw" / "market" / "zips",
        base_dir / "datasets" / "db1b" / "raw" / "market",
        base_dir / "DB1B Zips",
        base_dir / "data" / "transtats_downloads",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert DB1BMarket TranStats ZIP/CSV exports to Parquet.")
    base_dir = Path(__file__).resolve().parents[1]
    parser.add_argument("--input-dir", type=Path, default=_default_input_dir(base_dir))
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=base_dir / "datasets" / "db1b" / "processed" / "market_parquet",
    )
    parser.add_argument(
        "--combined-out",
        type=Path,
        default=base_dir / "datasets" / "db1b" / "processed" / "db1b.parquet",
    )
    parser.add_argument("--no-combine", action="store_true", help="Skip writing the combined DB1B parquet")
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not delete existing db1b_market_*.parquet files in --out-dir before writing.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Write raw itinerary rows (much larger) instead of aggregating by carrier+OD+quarter.",
    )
    parser.add_argument("--domestic-only", action="store_true", help="Keep OriginCountry=US and DestCountry=US only")
    parser.add_argument("--filter-state", type=str, default=None, help="2-letter state (e.g., NY) to keep ODs touching the state")
    args = parser.parse_args()

    input_dir: Path = args.input_dir
    out_dir: Path = args.out_dir
    combined_out: Path = args.combined_out

    inputs = _dedupe_inputs(_collect_inputs(input_dir))
    if not inputs:
        raise SystemExit(f"No DB1BMarket .zip/.csv inputs found under {input_dir}")

    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError as exc:
        raise SystemExit("Missing dependency: duckdb (try `pip install duckdb`).") from exc

    con = duckdb.connect(database=":memory:")

    out_dir.mkdir(parents=True, exist_ok=True)
    if not args.no_clean:
        for path in out_dir.glob("db1b_market_*.parquet"):
            try:
                path.unlink()
            except OSError:
                pass

    extract_dir = out_dir / "_extract"
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir(parents=True, exist_ok=True)

    succeeded = 0
    failed = 0
    total_rows = 0
    per_source_parquets: list[Path] = []

    print(f"Found {len(inputs)} DB1BMarket input(s) under {input_dir}")
    for i, source_path in enumerate(inputs, start=1):
        year, quarter = _parse_year_quarter(source_path)
        suffix = ""
        if args.filter_state:
            suffix += f"_{args.filter_state.strip().lower()}"
        if args.domestic_only:
            suffix += "_us"

        if year and quarter:
            out_name = f"db1b_market_{year}_q{quarter}{suffix}.parquet"
        else:
            out_name = f"{source_path.stem.lower()}{suffix}.parquet"
        out_path = out_dir / out_name

        try:
            display_out = out_path.relative_to(base_dir)
        except ValueError:
            display_out = out_path
        print(f"[{i}/{len(inputs)}] {source_path.name} -> {display_out}")
        try:
            if source_path.suffix.lower() == ".zip":
                rows = _convert_one_zip(
                    con,
                    source_path,
                    out_path=out_path,
                    extract_dir=extract_dir,
                    domestic_only=args.domestic_only,
                    filter_state=args.filter_state,
                    aggregate=not args.raw,
                )
            else:
                rows = _convert_one_csv(
                    con,
                    source_path,
                    out_path=out_path,
                    domestic_only=args.domestic_only,
                    filter_state=args.filter_state,
                    aggregate=not args.raw,
                )
        except Exception as exc:
            failed += 1
            print(f"  Failed: {exc}")
            continue

        succeeded += 1
        total_rows += rows
        per_source_parquets.append(out_path)
        print(f"  Wrote {rows:,} rows")

    if extract_dir.exists():
        shutil.rmtree(extract_dir, ignore_errors=True)

    if succeeded == 0:
        print(f"Summary: succeeded=0 failed={failed} total_rows=0")
        return 1

    if not args.no_combine:
        combined_out.parent.mkdir(parents=True, exist_ok=True)
        if combined_out.exists():
            combined_out.unlink()
        combined_literal = str(combined_out).replace("'", "''")
        escaped_paths = [str(path).replace("'", "''") for path in per_source_parquets]
        files_list = ", ".join("'" + path + "'" for path in escaped_paths)
        con.execute(
            f"COPY (SELECT * FROM parquet_scan([{files_list}])) TO '{combined_literal}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        combined_rows = int(con.execute("SELECT COUNT(*) FROM parquet_scan(?)", [str(combined_out)]).fetchone()[0])
        print(f"Combined: wrote {combined_rows:,} rows -> {combined_out.relative_to(base_dir)}")

    print(f"Summary: succeeded={succeeded} failed={failed} total_rows={total_rows:,}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
