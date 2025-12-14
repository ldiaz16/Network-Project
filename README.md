# Airline Route Optimizer

Decision-support toolkit for comparing airline networks, surfacing competing routes, and simulating CBSA-aligned growth opportunities. Ships a shared analytics core, HTTP APIs (FastAPI + Flask), a CLI, and a static React frontend.

## What it does
- Compare two airlines: route-level scorecards, competition and market share, fleet utilization summaries.
- CBSA simulation: propose CBSA-similar routes based on best-performing network segments, with optional cache building.
- Fleet tools: fleet profiles, live fleet-assignment simulation, and “optimal aircraft for a route” suggestions.
- Delivery options: CLI (`main.py`), FastAPI (`src/api.py`), Flask+Gunicorn (`backend/app.py`), and a static UI (`frontend/`).

## Data prerequisites
- Routes (required): `T_T100_SEGMENT_ALL_CARRIER.csv` in the repo root.
- Lookup tables (required): `Lookup Tables/L_UNIQUE_CARRIERS.csv` and `Lookup Tables/L_AIRPORT.csv` (shipped in this repo).
- CBSA lookup (required for CBSA simulation): `data/cbsa.csv`.
- Optional (recommended): airport coordinates via `data/airports.csv` (OurAirports format with `iata_code`, `latitude_deg`, `longitude_deg`, etc.) to enable geodesic distances and CBSA lookups.
- Optional BTS enrichments (drop into `data/`): `db1b.parquet` or `.csv`. These unlock profitability context when present.
- Other helpers: fleet seating assumptions in `data/aircraft_config.py`, codeshare overrides in `data/codeshare_overrides.json`, cached CBSA lookups in `data/cbsa_cache.json`.

## Repository layout
- `src/` – domain logic (`backend_service.py`), data loading/scoring (`load_data.py`), FastAPI adapter (`api.py`), CORS/rate limiting/logging helpers.
- `backend/` – Flask entrypoint serving the same API and the static UI; suited for Gunicorn.
- `frontend/` – static React bundle (no build step required) that talks to the API base defined by `<meta name="api-base">`.
- `scripts/` – reporting (`report.py`, `showcase.py`), BTS converters (`convert_bts_asc.py`, `convert_db20.py`, etc.), ASM dashboards/checks, load testing, and API-base setter.
- `tests/` – pytest coverage for data utils, CORS config, and core backend flows.
- `data/` – place required/optional datasets here; large raw archives are ignored by Docker and git.
- `docs/ARCHITECTURE.md` – high-level system diagram and flow notes.

## Local setup (Python)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
make data               # validate BTS dataset presence
```

### Run options
- FastAPI (dev): `uvicorn src.api:app --reload` → http://localhost:8000/api
- Flask (prod-friendly): `python backend/app.py` or `gunicorn backend.app:app --bind 0.0.0.0:8000`
- CLI workflow:  
  `python main.py --airline "Delta Air Lines" --airline "United Airlines"`  
  Add `--cbsa-airline` for CBSA-only runs, `--skip-comparison` to bypass the two-airline workflow, `--build-cbsa-cache` to precompute CBSA metadata.
- Frontend: serve `frontend/` statically, e.g. `python -m http.server 5173 --directory frontend`. Set the API base with `python scripts/set_frontend_api_base.py --api-base "http://localhost:8000/api"`.

## Docker / Compose
Run API + static frontend without installing Python locally:
```bash
docker compose --progress plain up --build
```
- API on `localhost:8000`, frontend on `localhost:5173`.
- `data/` is mounted read-only; large raw archives (e.g., `BTS DATA/`, `.asc` dumps) are ignored by `.dockerignore` and stay on the host.

## Configuration
- CORS: override defaults with `CORS_ALLOW_ORIGINS` and `CORS_ALLOW_ORIGIN_REGEXES` (comma-separated). Defaults allow local dev ports and Vercel domains.
- Rate limiting: `RATE_LIMIT_PER_MINUTE` (default 120). Applied per-client in both API adapters.
- Logging: structured JSON via `src/logging_setup.py`; request latency surfaced via `X-Request-Latency-ms`.

## Tests
```bash
pytest
```

## Useful scripts
- Reports: `python scripts/report.py --output-markdown reports/demo_report.md --output-pdf reports/demo_report.pdf`
- ASM quality/dashboard: `python scripts/check_asm_quality.py ...`, `python scripts/asm_dashboard.py ...`
- Data converters: `python scripts/convert_bts_asc.py ...`, `python scripts/convert_db20.py ...`, `python scripts/convert_db28.py ...`, `python scripts/convert_fuel.py ...`
- Load test: `python scripts/load_test_runner.py --base-url http://localhost:8000 --requests 100 --concurrency 10`
- API base injector for the static UI: `python scripts/set_frontend_api_base.py --api-base "..."`

## Make targets
- `make data` – validate BTS datasets are present.
- `make api` – run FastAPI with reload.
- `make report` – render demo report (writes to `reports/`).
- `make asm-dashboard` / `make asm-check` – visualize or gate ASM data quality.

## API surface (shortlist)
- `GET /health` – readiness probe.
- `GET /api/airlines?query=` – fuzzy airline search.
- `POST /api/run` – comparison + CBSA pipeline.
- `GET /api/fleet` – fleet profile for an airline.
- `POST /api/fleet-assignment` – simulate day-of flying with a supplied fleet mix.
- `POST /api/route-share` – market-share snapshot for specific routes.
- Flask-only extras: `POST /api/optimal-aircraft`, `POST /api/propose-route`.

## Performance & scale notes
- BTS T-100 segment exports can be large; API startup parses and aggregates `T_T100_SEGMENT_ALL_CARRIER.csv` on boot.
- Architecture: service-style domain layer (`src/backend_service.py`) behind adapters (FastAPI async endpoints with threadpool offloading; Flask + Gunicorn), with shared `DataStore` for preprocessing/scoring and request-level rate limiting/CORS.
- Benchmarks: run `python scripts/load_test_runner.py --base-url http://localhost:8000 --requests 100 --concurrency 10` while the API is up to capture throughput and latency (p50/p95). Results are environment-dependent; record them in your deployment notes or CI artifacts if you need measurable improvements over time.

## Notes for production
- Keep `data/` on a persistent volume; DB1B and CBSA caches are optional but benefit from persistence.
- Set explicit CORS origins, tighten rate limits, and front Gunicorn/Uvicorn with a reverse proxy for TLS and buffering.
- Use the Docker image as the deployable unit; mount `data/` read-only in runtimes that need BTS files.
