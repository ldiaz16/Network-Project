import os
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from pydantic import ValidationError

from src.backend_service import (
    AnalysisError,
    AnalysisRequest,
    list_airlines as list_airlines_logic,
    run_analysis as run_analysis_logic,
)
from src.cors_config import get_cors_settings
from src.load_data import DataStore

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

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
CORS(app, resources={r"/api/*": {"origins": cors_origins}}, supports_credentials=True)

data_store = DataStore()
data_store.load_data()


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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
