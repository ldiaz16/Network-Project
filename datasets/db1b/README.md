# DB1B (TranStats) data

This folder keeps DB1B-related inputs/outputs out of the repo root.

## Layout

- `raw/market/zips/`: DB1BMarket `.zip` downloads.
- `raw/market/csv/`: extracted DB1BMarket `.csv` files (often huge).
- `raw/lookup_tables/`: TranStats lookup tables used to interpret DB1B columns.
- `processed/`: generated Parquet outputs used by the app:
  - `processed/db1b.parquet` (combined; auto-loaded by the app if present)
  - `processed/market_parquet/` (per-quarter Parquet files)

## Convert to Parquet

From the repo root:

```bash
python3 scripts/convert_db1b_market_zips.py --domestic-only
```

The converter defaults to `raw/market/zips/`. To process extracted CSVs, run with `--input-dir datasets/db1b/raw/market/csv`, or point at `raw/market/` to include both. If both a ZIP and CSV exist for the same year/quarter, the ZIP is preferred to avoid double-processing.
If you have additional DB1BMarket CSV folders elsewhere under `datasets/db1b/` (e.g., `datasets/db1b/Origin_and_Destination_Survey_DB1BMarket_YYYY_Q/`), run with `--input-dir datasets/db1b` to pick them up.

## Domestic Demand Mart (canonical)

Once `processed/db1b.parquet` exists, build the quarterly domestic O&D demand/yield mart:

```bash
python3 scripts/build_domestic_demand_mart.py \
  --db1b-path datasets/db1b/processed/db1b.parquet \
  --out datasets/db1b/processed/domestic_demand_mart.parquet
```

## Market ranking & stability (Phase 2)

Generate top markets, concentration, and stability classifications (writes to `reports/`):

```bash
python3 scripts/demand_market_analysis.py --since-year 2022 --top-n 50
```
