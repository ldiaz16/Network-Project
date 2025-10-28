import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from src.backend_service import (
    AnalysisError,
    AnalysisRequest,
    AirlineSearchResponse,
    list_airlines as list_airlines_logic,
    run_analysis as run_analysis_logic,
)
from src.load_data import DataStore


app = FastAPI(
    title="Airline Route Optimizer API",
    description="HTTP API for running airline comparison and CBSA opportunity simulations.",
    version="0.1.0",
)

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
origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

data_store = DataStore()
data_store.load_data()


@app.get("/api/airlines", response_model=List[AirlineSearchResponse])
def list_airlines(query: Optional[str] = Query(default=None, description="Filter airlines by case-insensitive substring match.")) -> List[Dict[str, Any]]:
    return list_airlines_logic(data_store, query)


@app.post("/api/run")
def run_analysis(payload: AnalysisRequest) -> Dict[str, Any]:
    try:
        return run_analysis_logic(data_store, payload)
    except AnalysisError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
