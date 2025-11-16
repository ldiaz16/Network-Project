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
from src.cors_config import combine_regex_patterns, get_cors_settings

app = FastAPI(
    title="Airline Route Optimizer API",
    description="HTTP API for running airline comparison and CBSA opportunity simulations.",
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


@app.get("/api/airlines", response_model=List[AirlineSearchResponse])
def list_airlines(query: Optional[str] = Query(default=None, description="Filter airlines by case-insensitive substring match.")) -> List[Dict[str, Any]]:
    return list_airlines_logic(data_store, query)


@app.post("/api/run")
def run_analysis(payload: AnalysisRequest) -> Dict[str, Any]:
    try:
        return run_analysis_logic(data_store, payload)
    except AnalysisError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
