import os

bind = f"0.0.0.0:{os.environ.get('PORT', '8000')}"

# Keep defaults conservative for small-memory hosts (e.g. Render free tier).
# Override via env vars when running on larger instances.
workers = int(os.environ.get("WEB_CONCURRENCY", "1") or 1)
threads = int(os.environ.get("GUNICORN_THREADS", "2") or 2)
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "90") or 90)

# Use threads instead of forking multiple worker processes to avoid duplicating
# large in-memory datasets.
worker_class = os.environ.get("GUNICORN_WORKER_CLASS", "gthread")
