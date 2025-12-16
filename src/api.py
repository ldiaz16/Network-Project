import logging
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from src.backend_service import (
    AllianceAnalysisRequest,
    AnalysisError,
    RouteAnalysisRequest,
    alliance_analysis,
    list_airlines,
    list_alliances,
    route_analysis,
)
from src.cors_config import combine_regex_patterns, get_cors_settings
from src.demand_service import DemandMartCache, get_concentration_summary, get_stability_page, get_top_markets
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
DEMAND_CACHE = DemandMartCache(base_dir=Path(__file__).resolve().parents[1])


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


@app.get("/api/alliances")
async def get_alliances() -> List[Dict[str, Any]]:
    return await run_in_threadpool(list_alliances, data_store)


@app.post("/api/analysis")
async def analyze(payload: RouteAnalysisRequest) -> Dict[str, Any]:
    try:
        return await run_in_threadpool(route_analysis, data_store, payload)
    except AnalysisError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.post("/api/alliance")
async def analyze_alliance(payload: AllianceAnalysisRequest) -> Dict[str, Any]:
    try:
        return await run_in_threadpool(alliance_analysis, data_store, payload)
    except AnalysisError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@app.get("/api/demand/markets/top")
async def demand_top_markets(
    since_year: int = Query(default=2022, ge=1900, le=2100),
    top_n: int = Query(default=50, ge=1, le=500),
    directional: bool = Query(default=False),
    exclude_big3: bool = Query(default=False),
) -> Dict[str, Any]:
    try:
        df = await run_in_threadpool(
            get_top_markets,
            DEMAND_CACHE,
            since_year=since_year,
            directional=directional,
            top_n=top_n,
            exclude_big3=exclude_big3,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "since_year": since_year,
        "directional": directional,
        "exclude_big3": exclude_big3,
        "top_n": top_n,
        "markets": df.to_dict(orient="records"),
    }


@app.get("/api/demand/markets/concentration")
async def demand_market_concentration(
    since_year: int = Query(default=2022, ge=1900, le=2100),
    directional: bool = Query(default=False),
) -> Dict[str, Any]:
    try:
        top_10 = await run_in_threadpool(
            get_concentration_summary,
            DEMAND_CACHE,
            since_year=since_year,
            directional=directional,
            top_share=0.10,
        )
        top_01 = await run_in_threadpool(
            get_concentration_summary,
            DEMAND_CACHE,
            since_year=since_year,
            directional=directional,
            top_share=0.01,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

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

    return {
        "since_year": since_year,
        "directional": directional,
        "top_10pct": _serialize(top_10),
        "top_1pct": _serialize(top_01),
    }


@app.get("/api/demand/markets/stability")
async def demand_market_stability(
    since_year: int = Query(default=2022, ge=1900, le=2100),
    directional: bool = Query(default=False),
    q: Optional[str] = Query(default=None),
    classification: Optional[str] = Query(default=None),
    min_total_passengers: float = Query(default=0.0, ge=0.0),
    sort_by: str = Query(default="total_passengers"),
    sort_dir: str = Query(default="desc"),
    offset: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=10000),
) -> Dict[str, Any]:
    try:
        payload = await run_in_threadpool(
            get_stability_page,
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
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    payload["since_year"] = since_year
    payload["directional"] = directional
    payload["q"] = (q or "").strip()
    payload["classification"] = (classification or "").strip()
    return payload
