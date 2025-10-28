import os

from flask import Flask, jsonify, request
from flask_cors import CORS
from pydantic import ValidationError

from src.backend_service import (
    AnalysisError,
    AnalysisRequest,
    list_airlines as list_airlines_logic,
    run_analysis as run_analysis_logic,
)
from src.load_data import DataStore

default_origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:4173",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:4173",
    "http://127.0.0.1:5173",
]

raw_origins = os.environ.get("CORS_ALLOW_ORIGINS", ",".join(default_origins))
allowed_origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()] or ["*"]

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": allowed_origins}}, supports_credentials=True)

data_store = DataStore()
data_store.load_data()


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
