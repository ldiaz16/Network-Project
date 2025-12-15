import logging
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from pydantic import ValidationError

from src.backend_service import AnalysisError, RouteAnalysisRequest, list_airlines, route_analysis
from src.cors_config import get_cors_settings
from src.load_data import DataStore
from src.logging_setup import setup_logging
from src.security import RateLimiter

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"

explicit_origins, regex_origins = get_cors_settings()
cors_origins = explicit_origins if explicit_origins == ["*"] else list(dict.fromkeys([*explicit_origins, *regex_origins]))

setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
CORS(app, resources={r"/api/*": {"origins": cors_origins}}, supports_credentials=True)

data_store = DataStore()
data_store.load_data()
rate_limiter = RateLimiter.from_env()


@app.before_request
def before_request():
    client = request.remote_addr or "unknown"
    if not rate_limiter.is_allowed(client):
        return jsonify({"detail": "Too many requests"}), 429
    request.start_time = time.perf_counter()


@app.after_request
def log_request(response):
    start = getattr(request, "start_time", None)
    latency_ms = 0.0
    if start is not None:
        latency_ms = (time.perf_counter() - start) * 1000
        response.headers["X-Request-Latency-ms"] = f"{latency_ms:.2f}"

    logger.info(
        "request",
        extra={
            "request_path": request.path,
            "method": request.method,
            "status_code": response.status_code,
            "latency_ms": round(latency_ms, 2),
            "client": request.remote_addr,
        },
    )
    return response


@app.get("/")
def index():
    return app.send_static_file("index.html")


@app.get("/favicon.ico")
def favicon():
    icon_path = FRONTEND_DIR / "favicon.ico"
    if icon_path.exists():
        return send_from_directory(app.static_folder, "favicon.ico")
    return ("", 204)


@app.get("/health")
def healthcheck():
    return jsonify({"status": "ok"})


@app.get("/api/airlines")
def airlines():
    query = request.args.get("query")
    return jsonify(list_airlines(data_store, query))


@app.post("/api/analysis")
def analysis():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = RouteAnalysisRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        result = route_analysis(data_store, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)
