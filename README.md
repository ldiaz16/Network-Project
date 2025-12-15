# Airline Route Optimizer

This toolkit is now a **T-100-first route lens**: it reads the BTS `T_T100_SEGMENT_ALL_CARRIER.csv` export from the repository root, summarizes an airline’s network, and shows its top ASM-powered routes. No additional datasets (DB1B, CBSA, etc.) are required for the core experience today.

## Data prerequisites
- The app reads the BTS T-100 export from the repo root:
  - `T_T100_SEGMENT_ALL_CARRIER.csv` (preferred if you have a fresh download), or
  - `T_T100_SEGMENT_ALL_CARRIER.csv.gz` (a compressed snapshot suitable for GitHub).
  The raw `.csv` is ignored by git because it is large and regularly refreshed.

## Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Verify data

```bash
make data
```

This script just ensures the lookup tables and the BTS T-100 export exist locally.

## Run the backend

```bash
uvicorn src.api:app --reload
```

The API exposes:
- `GET /api/airlines?query=` – fuzzy airline search (returns name, alias, IATA, country).
- `POST /api/analysis` – run a single-airline route analysis using only the T-100 export.

## Serve the frontend

```bash
python -m http.server 5173 --directory frontend
```

Open http://localhost:5173 and enter an airline to inspect its routes, network stats, and top carriers by ASM.

## Notes
- The analysis focuses on the BTS T-100 segments dataset. Route scores, hubs, and equipment summaries are derived from that single source.
- CASM or profitability proxies are not calculated in this release—just route counts, distances, ASMs, and network reach.
