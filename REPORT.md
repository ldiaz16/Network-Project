# Airline P/L Forecasting: Summary and Results

## What this project does
- **Monthly operating P/L forecasting** using classical time-series models with airline ops and fuel as exogenous drivers.
- **Per-ASM normalization** to stabilize scale: model P/L/ASM, then convert back to dollars with an ASM forecast.
- **Exogenous signals**: Operating_Revenue, ASM, jet fuel, load factor, passengers, RPM, plus **yield** features (revenue per RPM/pax/ASM).
- **Shock handling**: event dummies for GFC and COVID; percentile-based spike flags.
- **Scenario-aware forecasting**: capped growth for exogenous series to prevent runaway drift.
- **Backtesting**: rolling 12-month forecasts on a 70/30 split with regime tagging (2015–2019, 2020+).
- **VAR baseline**: multivariate per-ASM model for comparison.
- **Visualization**: forecast plots, yield vs P/L scatter/trend.
- **Frontend UI**: lightweight browser app (`frontend/`) that talks to the FastAPI backend for route/network analysis and CBSA simulations; supports autocomplete, comparison of two airlines, CBSA-only runs, and displays network/route results.

### Frontend feature set
- Airline comparison: side-by-side network stats, top routes, competition mix, maturity, market share, and fleet utilization.
- Route finder: best-route suggestions and route share results aligned to network aggressiveness and competition posture.
- CBSA simulations: run CBSA-only or mixed workflows with cache-building, parameter controls, and corridor suggestions.
- Autocomplete inputs, skip-comparison mode, and message panel surfacing API/validation feedback.

### Data engineering notes (for SWE/DE internship context)
- **Pipelines/scripts**: ingestion and feature builders in `scripts/` (monthly aggregation, fuel conversion, ARIMAX/VAR backtests, plotting). Targets per-ASM normalization, yield computation, and exogenous capping.
- **Data storage**: Parquet files under `data/` (e.g., `us_passenger_monthly_with_yield.parquet`) for columnar, compressed access; CSV/PNG reports under `reports/`.
- **Backtesting harness**: rolling 12-month forecasts with regime tagging; produces reproducible CSVs for offline evaluation.
- **API contracts**: frontend consumes FastAPI at `/api`; form inputs map to backend analysis endpoints (route finder, airline comparison, CBSA simulation).
- **Reproducibility**: scripts runnable with `python3` (no pinned env here; see `requirements.txt`), outputs written deterministically to `reports/` for inspection.

### UX walkthrough (frontend)
1) **API base**: defaults to `http://localhost:8000/api` or a meta tag; can be overridden via `window.API_BASE`.
2) **Airline inputs**: type airline names to get autocomplete suggestions. You can toggle “Skip comparison” to analyze a single airline.
3) **CBSA parameters** (optional): enter a list of airlines (one per line), tweak top-N/suggestion counts, and choose whether to rebuild CBSA cache (with country/limit/chunk size controls).
4) **Run Analysis**: click “Run Analysis.” The lower panel populates with:
   - Network summary (ASM, routes, maturity bands, competition mix, yield proxy).
   - Top routes and route share tables (best routes aligned to network aggressiveness).
   - Market share snippets, fleet utilization snapshot.
   - CBSA corridor suggestions (if requested).
5) **Messages area**: shows API/validation feedback (e.g., missing airlines, cache status). Errors/notes appear inline above results.
6) **Tabs/sections**: use the tabs and cards to navigate route-level details, fleet stats, and CBSA results without rerunning the form.

## Key scripts
- `scripts/pnl_forecast_arimax.py`: ARIMAX forecast (per-ASM target, exog with yield/fuel/ops; outputs PNG/CSV).
- `scripts/backtest_monthly.py`: rolling backtest for OLS and ARIMAX with regime tags; outputs CSV metrics.
- `scripts/var_pl.py`: VAR backtest on per-ASM series; outputs CSV and MAE/SMAPE printout.
- `scripts/plot_yield_vs_pl.py`: scatter of yield vs P/L with trendline.
- Frontend: `frontend/index.html`, `frontend/app.js`, `frontend/styles.css` (served as static files; see `frontend/README.md`).

## Data
- Primary input: `data/us_passenger_monthly_with_yield.parquet` (monthly ops/financials with computed yield columns).
- Outputs: `reports/pnl_forecast_arimax.{png,csv}`, `reports/backtest_monthly.csv`, `reports/backtest_var.csv`, `reports/yield_vs_pl.png`.
- Frontend consumes backend API at `http://localhost:8000/api`; can be served via `python -m http.server 5173 --directory frontend` (see `frontend/README.md`).

## Current performance (backtest, dollars)
- **ARIMAX**: MAE ≈ **1.21M**, SMAPE ≈ **0.80** (overall).
  - 2015–2019 regime: MAE ≈ **0.24M** (strong fit).
  - 2020+ regime: MAE ≈ **1.40M** (improved vs OLS but still volatile).
- **OLS baseline**: MAE ≈ 1.61M, SMAPE ≈ 1.54.
- **VAR (per-ASM)**: MAE ≈ 0.027 (per ASM), SMAPE ≈ 1.36 (per-ASM scale; not directly comparable to dollars).

## Notable improvements
- Added yield features to exogenous set (revenue per RPM/pax/ASM).
- Stabilized targets via P/L per ASM and reconversion using ASM forecasts.
- Added shock dummies (GFC, COVID) to reduce lag on major events.
- Introduced capped growth for exogenous forecasts to limit drift.
- Added regime tagging to surface performance by period.

## Next enhancement ideas
- Use richer exogenous forecasts (fuel forward curves, macro, competitive intensity).
- Add more event dummies (fuel spikes, strikes) and regime-specific caps.
- Try alternate models: dollar-scale VAR with ASM reconversion, tree/GBM models on lags + exog, or profit margin targets.
