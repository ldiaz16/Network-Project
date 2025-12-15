# Deployment Guide

This project now ships with a Flask-powered backend (`backend/app.py`) and a static frontend (`frontend/`) suitable for Vercel. Follow the steps below to publish both services.

## Backend (Flask + Gunicorn)

1. **Dependencies**  
   - Use `requirements.txt` (Python ≥ 3.10).  
   - `runtime.txt` pins Python 3.11.6 for platforms that honour it (e.g. Render/Heroku).

2. **Local smoke test**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   python3 backend/app.py  # serves on http://localhost:8000
   ```

3. **Production command**  
   - A `Procfile` is included: `web: gunicorn backend.app:app --bind 0.0.0.0:$PORT`.  
   - Ensure the `data/` directory ships with the deployment – it contains all precomputed datasets.

4. **Example: Render Web Service**
   - New Web Service → select this repository.
   - Build command: `pip install -r requirements.txt`.
   - Start command: `gunicorn backend.app:app --bind 0.0.0.0:$PORT`.
   - Environment variables:  
     `PYTHON_VERSION=3.11.6` (optional but keeps parity with local runs).  
     `CORS_ALLOW_ORIGINS=https://<your-custom-domain>` (comma-separated list; only needed when you want stricter control, because the default now permits localhost plus any `*.vercel.app` domain).  
     `CORS_ALLOW_ORIGIN_REGEXES=https://(.+\.)?example\.com` (optional comma-separated regex list; defaults to `https://(.+\.)?vercel\.app` so any Vercel preview/production site can reach the API).
   - Once live, note the service URL; the API surface is exposed at `<service-url>/api/...`.

Any other WSGI-friendly host (Railway, Fly.io, traditional VPS) can use the same start command.

## Frontend (Static → Vercel)

1. **API base configuration**  
   The SPA reads a `<meta name="api-base">` tag to determine which backend to call. Use the helper script to inject the production URL:
   ```bash
   python3 scripts/set_frontend_api_base.py --api-base "https://your-backend-domain/api"
   ```
   For local development leave it at the default (`http://localhost:8000/api`).

2. **Vercel setup**
   - Create a new Vercel project and point it to this repository.
   - Set the project root to the repository root.
   - Configure the build step so the API base is injected automatically:
     - Build command: `python3 scripts/set_frontend_api_base.py --api-base "$API_BASE_URL"`
     - Output directory: `frontend`
   - Define an Environment Variable `API_BASE_URL` with your deployed backend’s `/api` base (e.g. `https://airline-backend.onrender.com/api`).

3. **Post-deploy validation**
   - After Vercel finishes, visit the site and submit an analysis to confirm calls hit the Flask backend.
   - If CORS errors appear, ensure the backend `origins` list includes the Vercel domain (update `backend/app.py` if needed).

## Useful Commands

```bash
# Run backend locally with hot reload via Flask's built-in server
FLASK_APP=backend/app.py FLASK_ENV=development flask run --port 8000

# Lint/update the frontend API base meta tag manually
python3 scripts/set_frontend_api_base.py --api-base "http://localhost:8000/api"
```

## ASM Data Quality Workflow

The shareholder-facing ASM summaries written by `scripts/report.py` and `scripts/showcase.py` now append every snapshot to `reports/asm_summary_history.csv`. Use the helpers below to keep that history actionable:

```bash
# Render a Markdown dashboard (reports/asm_dashboard.md) from the history log
make asm-dashboard

# Enforce alert thresholds (fails when estimates/unknown data dominate)
make asm-check
```

`scripts/check_asm_quality.py` is CI-friendly: it prints alerts for each airline and exits with status 1 when `--fail-on-alert` is present. Run the command after generating fresh ASM summaries in your pipeline to stop regressions before they ship.

With both services online, the frontend will call `GET /api/airlines` for autocomplete and `POST /api/run` for the full analysis pipeline.
