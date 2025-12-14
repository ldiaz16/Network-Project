import logging
import os
import time
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from pydantic import ValidationError

from src.backend_service import (
    AnalysisError,
    AnalysisRequest,
    FleetAssignmentRequest,
    OptimalAircraftRequest,
    ProposedRouteRequest,
    RouteShareRequest,
    analyze_route_market_share,
    get_airline_fleet_profile,
    list_airlines as list_airlines_logic,
    recommend_optimal_aircraft,
    run_analysis as run_analysis_logic,
    propose_route as propose_route_logic,
    simulate_live_assignment,
)
from src.cors_config import get_cors_settings
from src.load_data import DataStore
from src.logging_setup import setup_logging
from src.security import RateLimiter

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"

explicit_origins, regex_origins = get_cors_settings()


def _dedupe(items):
    seen = set()
    ordered = []
    for entry in items:
        if entry not in seen:
            ordered.append(entry)
            seen.add(entry)
    return ordered


if explicit_origins == ["*"]:
    cors_origins = ["*"]
else:
    cors_origins = _dedupe([*explicit_origins, *regex_origins])

setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
CORS(app, resources={r"/api/*": {"origins": cors_origins}}, supports_credentials=True)

data_store = DataStore()
data_store.load_data()
rate_limiter = RateLimiter.from_env()


@app.before_request
def _rate_limit_and_track_start():
    client = request.remote_addr or "unknown"
    if not rate_limiter.is_allowed(client):
        return jsonify({"detail": "Too many requests"}), 429
    request.start_time = time.perf_counter()


@app.after_request
def _log_request(response):
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
    # Serve the prebuilt static UI.
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
def list_airlines():
    query = request.args.get("query")
    results = list_airlines_logic(data_store, query)
    return jsonify(results)


@app.post("/api/run")
def run_analysis():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = AnalysisRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        result = run_analysis_logic(data_store, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)


@app.post("/api/optimal-aircraft")
def optimal_aircraft():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = OptimalAircraftRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        result = recommend_optimal_aircraft(data_store, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)


@app.get("/api/fleet")
def fleet_profile():
    query = request.args.get("airline", "")
    try:
        result = get_airline_fleet_profile(data_store, query)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code
    return jsonify(result)


@app.post("/api/fleet-assignment")
def fleet_assignment():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = FleetAssignmentRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        result = simulate_live_assignment(data_store, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)


@app.post("/api/route-share")
def route_share():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = RouteShareRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        result = analyze_route_market_share(data_store, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)


@app.post("/api/propose-route")
def propose_route():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = ProposedRouteRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        result = propose_route_logic(data_store, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
