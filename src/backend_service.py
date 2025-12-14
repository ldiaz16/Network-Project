"""Shared backend domain logic for both FastAPI and Flask adapters."""

from typing import Any, Dict, List, Optional, Set

import pandas as pd
from pydantic import BaseModel, Field, validator


def _dataframe_to_records(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    """Convert a DataFrame to a list of JSON-serialisable records."""
    if df is None or df.empty:
        return []
    mapped = df.copy()
    # Prefer display-friendly labels when present.
    if {"Source airport", "Source airport Display"}.issubset(mapped.columns):
        mapped["Source airport"] = mapped["Source airport Display"].where(mapped["Source airport Display"].notna(), mapped["Source airport"])
    if {"Destination airport", "Destination airport Display"}.issubset(mapped.columns):
        mapped["Destination airport"] = mapped["Destination airport Display"].where(
            mapped["Destination airport Display"].notna(), mapped["Destination airport"]
        )
    if {"Equipment", "Equipment Display"}.issubset(mapped.columns):
        mapped["Equipment"] = mapped["Equipment Display"].where(mapped["Equipment Display"].notna(), mapped["Equipment"])
    return (
        mapped.astype(object)
        .where(pd.notna(mapped), None)
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


class OptimalAircraftRequest(BaseModel):
    airline: str
    route_distance: float = Field(..., gt=0)
    seat_demand: Optional[int] = None
    top_n: int = Field(default=3, ge=1)

    @validator("airline", pre=True, always=True)
    def _require_airline(cls, value: Any) -> str:
        if value is None:
            raise ValueError("Airline name is required.")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Airline name is required.")
        return normalized

    @validator("seat_demand")
    def _validate_seat_demand(cls, value: Optional[int]) -> Optional[int]:
        if value is None:
            return None
        if value <= 0:
            raise ValueError("Seat demand must be a positive number.")
        return value


class FleetConfigEntry(BaseModel):
    equipment: str
    count: int = Field(..., ge=1)

    @validator("equipment", pre=True, always=True)
    def _require_equipment(cls, value: Any) -> str:
        if value is None:
            raise ValueError("Equipment name is required.")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Equipment name is required.")
        return normalized.upper()


class FleetAssignmentRequest(BaseModel):
    airline: str
    fleet: List[FleetConfigEntry]
    route_limit: int = Field(default=60, ge=1, le=500)
    day_hours: float = Field(default=18.0, gt=0)
    maintenance_hours: float = Field(default=6.0, ge=0)
    crew_max_hours: float = Field(default=14.0, gt=0)
    taxi_buffer_minutes: int = Field(default=20, ge=0)
    default_turn_minutes: int = Field(default=45, ge=0)

    @validator("airline", pre=True, always=True)
    def _require_airline(cls, value: Any) -> str:
        if value is None:
            raise ValueError("Airline name is required.")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("Airline name is required.")
        return normalized

    @validator("fleet")
    def _require_fleet(cls, value: List[FleetConfigEntry]) -> List[FleetConfigEntry]:
        if not value:
            raise ValueError("Provide at least one fleet entry.")
        return value

    @validator("maintenance_hours")
    def _validate_maintenance(cls, value: float, values: Dict[str, Any]) -> float:
        day_hours = values.get("day_hours", 0)
        if day_hours and value >= day_hours:
            raise ValueError("Maintenance window must be shorter than the operating day.")
        return value


class AirlineSearchResponse(BaseModel):
    airline: str
    alias: Optional[str]
    iata: Optional[str]
    country: Optional[str]


class RouteShareEntry(BaseModel):
    source: str
    destination: str

    @validator("source", "destination", pre=True, always=True)
    def _require_airport_code(cls, value: Any) -> str:
        if value is None:
            raise ValueError("Airport code is required.")
        normalized = str(value).strip().upper()
        if not normalized:
            raise ValueError("Airport code is required.")
        return normalized


class RouteShareRequest(BaseModel):
    routes: List[RouteShareEntry]
    top_airlines: int = Field(default=5, ge=1, le=20)
    include_all_competitors: bool = True

    @validator("routes")
    def _require_routes(cls, value: List[RouteShareEntry]) -> List[RouteShareEntry]:
        if not value:
            raise ValueError("At least one route is required.")
        return value


class ProposedRouteRequest(BaseModel):
    airline: str
    source: str
    destination: str
    seat_demand: Optional[float] = None

    @validator("airline", "source", "destination", pre=True, always=True)
    def _require_fields(cls, value: Any) -> str:
        if value is None:
            raise ValueError("This field is required.")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError("This field is required.")
        return normalized

    @validator("source", "destination")
    def _uppercase_airports(cls, value: str) -> str:
        return value.upper()


def _build_airline_package(data_store, query: str) -> Dict[str, Any]:
    """Create a reusable package containing core dataframes for an airline."""
    routes_df, metadata = data_store.select_airline_routes(query, verbose=False)
    if routes_df.empty:
        raise ValueError("Airline has no routes available for analysis.")

    processed_df = data_store.process_routes(routes_df)
    cost_df = data_store.cost_analysis(processed_df)
    scorecard = data_store.build_route_scorecard(cost_df)
    # Use full market ASM for share; avoids showing 100% share when competitors exist.
    market_share = data_store.compute_market_share_snapshot(cost_df, include_all_competitors=True)
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


def _prepare_top_routes(cost_df: Optional[pd.DataFrame], limit: int = 15) -> List[Dict[str, Any]]:
    """Return the heaviest ASM routes for quick reference on the fleet page."""
    if cost_df is None or cost_df.empty:
        return []
    columns = [
        "Source airport",
        "Destination airport",
        "Equipment",
        "Distance (miles)",
        "Total Seats",
        "ASM",
        "Competition Level",
        "Route Strategy Baseline",
        "Route Maturity Label",
    ]
    available_columns = [column for column in columns if column in cost_df.columns]
    if not available_columns:
        return []
    snapshot = (
        cost_df.sort_values("ASM", ascending=False)
        .head(limit)
        .loc[:, available_columns]
        .copy()
    )
    # Prefer human-readable labels when available.
    if "Source airport Display" in cost_df.columns:
        snapshot["Source airport"] = cost_df.loc[snapshot.index, "Source airport Display"]
    if "Destination airport Display" in cost_df.columns:
        snapshot["Destination airport"] = cost_df.loc[snapshot.index, "Destination airport Display"]
    if "Equipment Display" in cost_df.columns:
        snapshot["Equipment"] = cost_df.loc[snapshot.index, "Equipment Display"]
    rename_map = {
        "Source airport": "Source",
        "Destination airport": "Destination",
        "Distance (miles)": "Distance (mi)",
        "Total Seats": "Seats",
        "Route Strategy Baseline": "Strategy Baseline",
        "Route Maturity Label": "Route Maturity",
    }
    snapshot.rename(columns={key: rename_map.get(key, key) for key in available_columns}, inplace=True)
    return _dataframe_to_records(snapshot)


def get_airline_fleet_profile(data_store, query: str) -> Dict[str, Any]:
    """Return a single-airline profile with fleet composition and route context."""
    if not isinstance(query, str) or not query.strip():
        raise AnalysisError(400, "Airline name is required.")
    try:
        package = _build_airline_package(data_store, query)
    except ValueError as exc:
        raise AnalysisError(404, str(exc)) from exc

    metadata = package.get("metadata")
    if metadata is None:
        metadata = {}
    elif hasattr(metadata, "to_dict"):
        metadata = metadata.to_dict()

    def _clean(value):
        if value is None:
            return None
        if isinstance(value, str):
            trimmed = value.strip()
            if not trimmed or trimmed.upper() == "\\N":
                return None
        if pd.isna(value):
            return None
        return value

    airline_info = {
        "name": package.get("name"),
        "normalized": package.get("normalized"),
        "alias": _clean(metadata.get("Alias")),
        "iata": _clean(metadata.get("IATA")),
        "icao": _clean(metadata.get("ICAO")),
        "country": _clean(metadata.get("Country")),
        "callsign": _clean(metadata.get("Callsign")),
        "active": _clean(metadata.get("Active")),
        "total_routes": _clean(metadata.get("Total Routes")),
    }

    network = data_store.build_network(package["processed"])
    network_stats = data_store.analyze_network(network, package.get("normalized") or package.get("name"), processed_routes=package.get("processed"))
    asm_summary = data_store.summarize_asm_sources(package["cost"])
    fleet_utilization = _dataframe_to_records(package.get("fleet_utilization"))
    market_share = _dataframe_to_records(package.get("market_share"))
    top_routes = _prepare_top_routes(package.get("cost"))

    return {
        "airline": airline_info,
        "network_stats": network_stats,
        "scorecard": package.get("scorecard"),
        "fleet_utilization": fleet_utilization,
        "market_share": market_share,
        "top_routes": top_routes,
        "asm_sources": _dataframe_to_records(asm_summary),
    }


def recommend_optimal_aircraft(data_store, payload: OptimalAircraftRequest) -> Dict[str, Any]:
    try:
        recommendations = data_store.find_best_aircraft_for_route(
            payload.airline,
            route_distance=payload.route_distance,
            seat_demand=payload.seat_demand,
            top_n=payload.top_n,
        )
    except ValueError as exc:
        raise AnalysisError(400, str(exc)) from exc

    return {
        "airline": payload.airline,
        "optimal_aircraft": _dataframe_to_records(recommendations),
    }


def simulate_live_assignment(data_store, payload: FleetAssignmentRequest) -> Dict[str, Any]:
    if not payload.fleet:
        raise AnalysisError(400, "Fleet definition cannot be empty.")

    try:
        result = data_store.simulate_fleet_assignment(
            airline_query=payload.airline,
            fleet_config=[{"equipment": entry.equipment, "count": entry.count} for entry in payload.fleet],
            route_limit=payload.route_limit,
            day_hours=payload.day_hours,
            maintenance_hours=payload.maintenance_hours,
            crew_max_hours=payload.crew_max_hours,
            taxi_buffer_minutes=payload.taxi_buffer_minutes,
            default_turn_minutes=payload.default_turn_minutes,
        )
    except ValueError as exc:
        raise AnalysisError(400, str(exc)) from exc

    return result


def analyze_route_market_share(data_store, payload: RouteShareRequest) -> Dict[str, Any]:
    try:
        route_pairs = [(entry.source, entry.destination) for entry in payload.routes]
        return data_store.analyze_route_market_share(
            route_pairs,
            top_airlines=payload.top_airlines,
            include_all_competitors=payload.include_all_competitors,
        )
    except ValueError as exc:
        raise AnalysisError(400, str(exc)) from exc


def propose_route(data_store, payload: ProposedRouteRequest) -> Dict[str, Any]:
    try:
        result = data_store.evaluate_route_opportunity(
            payload.airline,
            payload.source,
            payload.destination,
            seat_demand=payload.seat_demand,
        )
        return result
    except ValueError as exc:
        raise AnalysisError(400, str(exc)) from exc


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


_AIRLINE_CACHE: Dict[str, List[Dict[str, Any]]] = {}


def list_airlines(data_store, query: Optional[str]) -> List[Dict[str, Any]]:
    """Return airline metadata filtered by optional substring query, with a small in-memory cache."""
    cache_key = (query or "").strip().lower()
    if cache_key in _AIRLINE_CACHE:
        return _AIRLINE_CACHE[cache_key]

    airlines_df = data_store.airlines.copy()
    if query:
        mask = airlines_df["Airline"].str.contains(query, case=False, na=False) | airlines_df["Alias"].str.contains(query, case=False, na=False)
        airlines_df = airlines_df[mask]
    subset = airlines_df.head(50)[["Airline", "Alias", "IATA", "Country"]]
    subset = subset.astype(object).where(pd.notna(subset), None)
    results = [
        {
            "airline": row["Airline"],
            "alias": row.get("Alias"),
            "iata": row.get("IATA"),
            "country": row.get("Country"),
        }
        for _, row in subset.iterrows()
    ]
    _AIRLINE_CACHE[cache_key] = results
    return results


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
        metadata = package.get("metadata")
        if hasattr(metadata, "to_dict"):
            metadata = metadata.to_dict()
        elif metadata is None:
            metadata = {}
        iata_code = metadata.get("IATA") or metadata.get("Airline Code") or "N/A"
        routes_df = package.get("routes")
        route_count = len(routes_df) if routes_df is not None else 0
        messages.append(f"Matched '{query}' to {package.get('name')} (IATA {iata_code}) with {route_count} routes.")
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
        airline_x_stats = data_store.analyze_network(
            airline_x_network,
            airline_x_pkg.get("normalized") or airline_x_pkg.get("name"),
            processed_routes=airline_x_pkg.get("processed"),
        )
        airline_y_stats = data_store.analyze_network(
            airline_y_network,
            airline_y_pkg.get("normalized") or airline_y_pkg.get("name"),
            processed_routes=airline_y_pkg.get("processed"),
        )

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
