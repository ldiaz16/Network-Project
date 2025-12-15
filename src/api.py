import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from src.backend_service import AnalysisError, RouteAnalysisRequest, list_airlines, route_analysis
from src.cors_config import combine_regex_patterns, get_cors_settings
from src.logging_setup import setup_logging
from src.load_data import DataStore
from src.security import RateLimiter

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Airline Route Optimizer (T-100 only)",
    description="Lightweight route analysis powered solely by the BTS T-100 segment export.",
    version="0.1.0",
)

explicit_origins, regex_origins = get_cors_settings()
allow_origin_regex = combine_regex_patterns(regex_origins)

app.add_middleware(
    CORSMiddleware,
    allow_origins=explicit_origins,
    allow_origin_regex=allow_origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

data_store = DataStore()
data_store.load_data()
rate_limiter = RateLimiter.from_env()


@app.middleware("http")
async def add_timing_and_rate_limit(request: Request, call_next):
    client_host = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_host):
        return JSONResponse({"detail": "Too many requests"}, status_code=429)

    start = time.perf_counter()
    response: Response = await call_next(request)
    latency_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Request-Latency-ms"] = f"{latency_ms:.2f}"

    logger.info(
        "request",
        extra={
            "request_path": request.url.path,
            "method": request.method,
            "status_code": response.status_code,
            "latency_ms": round(latency_ms, 2),
            "client": client_host,
        },
    )
    return response


class AirlineSearchResponse(BaseModel):
    airline: str
    alias: Optional[str]
    iata: Optional[str]
    country: Optional[str]


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/airlines", response_model=List[AirlineSearchResponse])
async def get_airlines(
    query: Optional[str] = Query(
        default=None, description="Filter airlines by substring match."
    )
) -> List[Dict[str, Any]]:
    return await run_in_threadpool(list_airlines, data_store, query)


@app.post("/api/analysis")
async def analyze(payload: RouteAnalysisRequest) -> Dict[str, Any]:
    try:
        return await run_in_threadpool(route_analysis, data_store, payload)
    except AnalysisError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc

