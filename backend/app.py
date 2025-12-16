import logging
import os
import time
import threading
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from pydantic import ValidationError

from src.backend_service import (
    AllianceAnalysisRequest,
    AnalysisError,
    RouteAnalysisRequest,
    alliance_analysis,
    list_airlines,
    list_alliances,
    route_analysis,
)
from src.cors_config import get_cors_settings
from src.load_data import DataStore
from src.logging_setup import setup_logging
from src.security import RateLimiter

FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"
BASE_DIR = Path(__file__).resolve().parents[1]
ENABLE_DB1B_DEMAND_API = os.environ.get("ENABLE_DB1B_DEMAND_API", "").strip().lower() in {"1", "true", "yes", "y"}
if ENABLE_DB1B_DEMAND_API:
    from src.demand_service import DemandMartCache, get_concentration_summary, get_stability_page, get_top_markets

    DEMAND_CACHE = DemandMartCache(base_dir=BASE_DIR)

explicit_origins, regex_origins = get_cors_settings()
cors_origins = explicit_origins if explicit_origins == ["*"] else list(dict.fromkeys([*explicit_origins, *regex_origins]))

setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder=str(FRONTEND_DIR), static_url_path="")
CORS(app, resources={r"/api/*": {"origins": cors_origins}}, supports_credentials=True)

data_store = DataStore()
_data_store_lock = threading.Lock()
_data_store_loaded = False
_data_store_error = None


def get_data_store() -> DataStore:
    global _data_store_loaded, _data_store_error
    if _data_store_loaded:
        return data_store
    if _data_store_error is not None:
        raise _data_store_error

    with _data_store_lock:
        if _data_store_loaded:
            return data_store
        try:
            data_store.load_data()
        except Exception as exc:
            _data_store_error = exc
            raise
        _data_store_loaded = True
        return data_store


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
    try:
        ds = get_data_store()
    except FileNotFoundError as exc:
        return jsonify({"detail": str(exc)}), 503
    except Exception as exc:
        return jsonify({"detail": str(exc)}), 500
    return jsonify(list_airlines(ds, query))


@app.get("/api/alliances")
def alliances():
    try:
        ds = get_data_store()
    except FileNotFoundError as exc:
        return jsonify({"detail": str(exc)}), 503
    except Exception as exc:
        return jsonify({"detail": str(exc)}), 500
    return jsonify(list_alliances(ds))


@app.post("/api/analysis")
def analysis():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = RouteAnalysisRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        ds = get_data_store()
    except FileNotFoundError as exc:
        return jsonify({"detail": str(exc)}), 503
    except Exception as exc:
        return jsonify({"detail": str(exc)}), 500

    try:
        result = route_analysis(ds, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)


@app.post("/api/alliance")
def alliance():
    payload = request.get_json(force=True, silent=True) or {}
    try:
        request_model = AllianceAnalysisRequest(**payload)
    except ValidationError as exc:
        return jsonify({"detail": exc.errors()}), 422

    try:
        ds = get_data_store()
    except FileNotFoundError as exc:
        return jsonify({"detail": str(exc)}), 503
    except Exception as exc:
        return jsonify({"detail": str(exc)}), 500

    try:
        result = alliance_analysis(ds, request_model)
    except AnalysisError as exc:
        return jsonify({"detail": str(exc)}), exc.status_code

    return jsonify(result)

if ENABLE_DB1B_DEMAND_API:
    @app.get("/api/demand/markets/top")
    def demand_top_markets():
        try:
            top_n = int(request.args.get("top_n", request.args.get("n", "50")))
        except ValueError:
            top_n = 50
        top_n = max(1, min(500, top_n))

        since_year_raw = request.args.get("since_year", "2022")
        try:
            since_year = int(since_year_raw)
        except ValueError:
            since_year = 2022

        directional = request.args.get("directional", "0").strip().lower() in {"1", "true", "yes", "y"}
        exclude_big3 = request.args.get("exclude_big3", "0").strip().lower() in {"1", "true", "yes", "y"}

        try:
            df = get_top_markets(
                DEMAND_CACHE,
                since_year=since_year,
                directional=directional,
                top_n=top_n,
                exclude_big3=exclude_big3,
            )
        except FileNotFoundError as exc:
            return jsonify({"detail": str(exc)}), 404
        except Exception as exc:
            return jsonify({"detail": str(exc)}), 500

        return jsonify(
            {
                "since_year": since_year,
                "directional": directional,
                "exclude_big3": exclude_big3,
                "top_n": top_n,
                "markets": df.to_dict(orient="records"),
            }
        )

    @app.get("/api/demand/markets/concentration")
    def demand_market_concentration():
        since_year_raw = request.args.get("since_year", "2022")
        try:
            since_year = int(since_year_raw)
        except ValueError:
            since_year = 2022

        directional = request.args.get("directional", "0").strip().lower() in {"1", "true", "yes", "y"}

        try:
            top_10 = get_concentration_summary(DEMAND_CACHE, since_year=since_year, directional=directional, top_share=0.10)
            top_01 = get_concentration_summary(DEMAND_CACHE, since_year=since_year, directional=directional, top_share=0.01)
        except FileNotFoundError as exc:
            return jsonify({"detail": str(exc)}), 404
        except Exception as exc:
            return jsonify({"detail": str(exc)}), 500

        def _serialize(stats):
            return {
                "markets": stats.markets,
                "total_passengers": stats.total_passengers,
                "top_share": stats.top_share,
                "top_markets": stats.top_markets,
                "top_passengers": stats.top_passengers,
                "top_passenger_share": stats.top_passenger_share,
                "long_tail_markets": stats.long_tail_markets,
                "long_tail_passengers": stats.long_tail_passengers,
                "long_tail_passenger_share": stats.long_tail_passenger_share,
            }

        return jsonify(
            {
                "since_year": since_year,
                "directional": directional,
                "top_10pct": _serialize(top_10),
                "top_1pct": _serialize(top_01),
            }
        )

    @app.get("/api/demand/markets/stability")
    def demand_market_stability():
        since_year_raw = request.args.get("since_year", "2022")
        try:
            since_year = int(since_year_raw)
        except ValueError:
            since_year = 2022

        directional = request.args.get("directional", "0").strip().lower() in {"1", "true", "yes", "y"}
        q = request.args.get("q")
        classification = request.args.get("classification")

        try:
            min_total_passengers = float(request.args.get("min_total_passengers", "0") or 0.0)
        except ValueError:
            min_total_passengers = 0.0

        sort_by = request.args.get("sort_by", "total_passengers")
        sort_dir = request.args.get("sort_dir", "desc")
        try:
            offset = int(request.args.get("offset", "0"))
        except ValueError:
            offset = 0
        try:
            limit = int(request.args.get("limit", "500"))
        except ValueError:
            limit = 500

        try:
            payload = get_stability_page(
                DEMAND_CACHE,
                since_year=since_year,
                directional=directional,
                q=q,
                classification=classification,
                min_total_passengers=min_total_passengers,
                sort_by=sort_by,
                sort_dir=sort_dir,
                offset=offset,
                limit=limit,
            )
        except FileNotFoundError as exc:
            return jsonify({"detail": str(exc)}), 404
        except Exception as exc:
            return jsonify({"detail": str(exc)}), 500

        payload["since_year"] = since_year
        payload["directional"] = directional
        payload["q"] = (q or "").strip()
        payload["classification"] = (classification or "").strip()
        return jsonify(payload)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
