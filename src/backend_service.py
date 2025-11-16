"""Shared backend domain logic for both FastAPI and Flask adapters."""

from typing import Any, Dict, List, Optional, Set

import pandas as pd
from pydantic import BaseModel, Field, validator


def _dataframe_to_records(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    """Convert a DataFrame to a list of JSON-serialisable records."""
    if df is None or df.empty:
        return []
    return (
        df.astype(object)
        .where(pd.notna(df), None)
        .to_dict(orient="records")
    )


class AnalysisError(Exception):
    """Exception raised when user input or processing fails."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


class AnalysisRequest(BaseModel):
    comparison_airlines: List[str] = Field(default_factory=list)
    skip_comparison: bool = False
    cbsa_airlines: List[str] = Field(default_factory=list)
    cbsa_top_n: int = 5
    cbsa_suggestions: int = 3
    build_cbsa_cache: bool = False
    cbsa_cache_limit: Optional[int] = Field(default=None, ge=1)
    cbsa_cache_chunk_size: Optional[int] = Field(default=200, ge=1)
    cbsa_cache_country: List[str] = Field(default_factory=list)

    @validator("comparison_airlines", "cbsa_airlines", "cbsa_cache_country", pre=True, each_item=True)
    def _strip_items(cls, value: str) -> str:
        if isinstance(value, str):
            return value.strip()
        return value

    @validator("comparison_airlines")
    def _drop_blank_comparison(cls, value: List[str]) -> List[str]:
        if not value:
            return []
        return [item for item in value if item]

    @validator("cbsa_airlines")
    def _drop_blank_cbsa(cls, value: List[str]) -> List[str]:
        if not value:
            return []
        return [item for item in value if item]

    @validator("cbsa_cache_country")
    def _drop_blank_countries(cls, value: List[str]) -> List[str]:
        if not value:
            return []
        return [item for item in value if item]


class AirlineSearchResponse(BaseModel):
    airline: str
    alias: Optional[str]
    iata: Optional[str]
    country: Optional[str]


def _build_airline_package(data_store, query: str) -> Dict[str, Any]:
    """Create a reusable package containing core dataframes for an airline."""
    routes_df, metadata = data_store.select_airline_routes(query, verbose=False)
    if routes_df.empty:
        raise ValueError("Airline has no routes available for analysis.")

    processed_df = data_store.process_routes(routes_df)
    cost_df = data_store.cost_analysis(processed_df)
    scorecard = data_store.build_route_scorecard(cost_df)
    market_share = data_store.compute_market_share_snapshot(cost_df)
    fleet_utilization = data_store.summarize_fleet_utilization(cost_df)
    airline_name = routes_df["Airline"].iloc[0]
    normalized_name = routes_df["Airline (Normalized)"].iloc[0]

    return {
        "query": query,
        "name": airline_name,
        "normalized": normalized_name,
        "routes": routes_df,
        "processed": processed_df,
        "cost": cost_df,
        "metadata": metadata,
        "scorecard": scorecard,
        "market_share": market_share,
        "fleet_utilization": fleet_utilization,
    }


def _run_cbsa_simulation(data_store, package: Dict[str, Any], args: AnalysisRequest) -> Dict[str, Any]:
    result = data_store.simulate_cbsa_route_opportunities(
        package["cost"],
        top_n=args.cbsa_top_n,
        max_suggestions_per_route=args.cbsa_suggestions,
    )
    return {
        "airline": package["name"],
        "best_routes": _dataframe_to_records(result.get("best_routes")),
        "suggestions": _dataframe_to_records(result.get("suggested_routes")),
        "scorecard": package.get("scorecard"),
        "market_share": _dataframe_to_records(package.get("market_share")),
        "fleet_utilization": _dataframe_to_records(package.get("fleet_utilization")),
    }


def list_airlines(data_store, query: Optional[str]) -> List[Dict[str, Any]]:
    """Return airline metadata filtered by optional substring query."""
    airlines_df = data_store.airlines.copy()
    if query:
        mask = airlines_df["Airline"].str.contains(query, case=False, na=False) | airlines_df["Alias"].str.contains(query, case=False, na=False)
        airlines_df = airlines_df[mask]
    subset = airlines_df.head(50)[["Airline", "Alias", "IATA", "Country"]]
    subset = subset.astype(object).where(pd.notna(subset), None)
    return [
        {
            "airline": row["Airline"],
            "alias": row.get("Alias"),
            "iata": row.get("IATA"),
            "country": row.get("Country"),
        }
        for _, row in subset.iterrows()
    ]


def run_analysis(data_store, payload: AnalysisRequest) -> Dict[str, Any]:
    """Execute the comparison and CBSA analysis pipeline."""
    messages: List[str] = []
    packages: Dict[str, Dict[str, Any]] = {}
    comparison_result: Optional[Dict[str, Any]] = None
    cbsa_results: List[Dict[str, Any]] = []
    cbsa_registry: List[Dict[str, Any]] = []

    def ensure_package(query: str) -> Dict[str, Any]:
        normalized_key = query.lower()
        if normalized_key in packages:
            return packages[normalized_key]
        package = _build_airline_package(data_store, query)
        packages[normalized_key] = package
        return package

    if payload.build_cbsa_cache:
        processed = data_store.build_cbsa_cache(
            countries=payload.cbsa_cache_country or None,
            limit=payload.cbsa_cache_limit,
            chunk_size=payload.cbsa_cache_chunk_size or 200,
        )
        messages.append(f"CBSA cache build complete. Processed {processed} airports.")

    comparison_airlines = payload.comparison_airlines
    if not payload.skip_comparison:
        if len(comparison_airlines) != 2:
            raise AnalysisError(400, "Exactly two airlines are required for comparison when skip_comparison is false.")

        try:
            airline_x_pkg = ensure_package(comparison_airlines[0])
            airline_y_pkg = ensure_package(comparison_airlines[1])
        except ValueError as exc:
            raise AnalysisError(400, str(exc)) from exc

        airline_x_network = data_store.build_network(airline_x_pkg["processed"])
        airline_y_network = data_store.build_network(airline_y_pkg["processed"])
        airline_x_stats = data_store.analyze_network(airline_x_network)
        airline_y_stats = data_store.analyze_network(airline_y_network)

        try:
            competing_routes_df = data_store.find_competing_routes(airline_x_pkg["cost"], airline_y_pkg["cost"])
        except Exception as exc:
            raise AnalysisError(500, f"Failed to compute competing routes: {exc}") from exc

        comparison_result = {
            "airlines": [
                {
                    "name": airline_x_pkg["name"],
                    "network_stats": airline_x_stats,
                    "scorecard": airline_x_pkg.get("scorecard"),
                    "market_share": _dataframe_to_records(airline_x_pkg.get("market_share")),
                    "fleet_utilization": _dataframe_to_records(airline_x_pkg.get("fleet_utilization")),
                },
                {
                    "name": airline_y_pkg["name"],
                    "network_stats": airline_y_stats,
                    "scorecard": airline_y_pkg.get("scorecard"),
                    "market_share": _dataframe_to_records(airline_y_pkg.get("market_share")),
                    "fleet_utilization": _dataframe_to_records(airline_y_pkg.get("fleet_utilization")),
                },
            ],
            "competing_routes": _dataframe_to_records(competing_routes_df),
        }
        cbsa_registry.extend([airline_x_pkg, airline_y_pkg])

    for query in payload.cbsa_airlines:
        try:
            pkg = ensure_package(query)
        except ValueError as exc:
            messages.append(f"Skipping CBSA simulation for '{query}': {exc}")
            continue
        cbsa_registry.append(pkg)

    processed_norms: Set[str] = set()
    for pkg in cbsa_registry:
        if pkg["normalized"] in processed_norms:
            continue
        processed_norms.add(pkg["normalized"])
        cbsa_results.append(_run_cbsa_simulation(data_store, pkg, payload))

    response: Dict[str, Any] = {
        "messages": messages,
        "cbsa": cbsa_results,
    }
    if comparison_result:
        response["comparison"] = comparison_result

    if not cbsa_results and not comparison_result:
        raise AnalysisError(400, "No analysis was performed. Provide airlines for comparison and/or CBSA simulation.")

    return response
