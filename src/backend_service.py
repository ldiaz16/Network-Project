"""Minimal backend helpers focused on T-100 route analysis."""

from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import BaseModel, Field


def _dataframe_to_records(df: Optional[pd.DataFrame]) -> List[Dict[str, Any]]:
    """Convert a DataFrame to JSON-friendly records."""
    if df is None or df.empty:
        return []
    normalized = df.astype(object).where(pd.notna(df), None)
    return normalized.to_dict(orient="records")


class AnalysisError(Exception):
    """Raised when a user request cannot be satisfied."""

    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


class RouteAnalysisRequest(BaseModel):
    airline: str = Field(..., description="Airline name, alias, or code to analyze.")
    limit: int = Field(20, ge=5, le=60, description="Maximum number of top routes to return.")


def _normalize_metadata(metadata: Any) -> Dict[str, Any]:
    if metadata is None:
        return {}
    if hasattr(metadata, "to_dict"):
        raw = metadata.to_dict()
    elif isinstance(metadata, dict):
        raw = metadata.copy()
    else:
        raw = dict(metadata)
    return {key: (value if pd.notna(value) else None) for key, value in raw.items()}


def _format_airline_metadata(raw: Dict[str, Any]) -> Dict[str, Any]:
    if not raw:
        return {}
    return {
        "name": raw.get("Airline"),
        "iata": raw.get("IATA"),
        "icao": raw.get("ICAO"),
        "country": raw.get("Country"),
        "alias": raw.get("Alias"),
        "normalized": raw.get("Airline (Normalized)"),
    }


def _safe_number(value: Any) -> float:
    try:
        candidate = float(value)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if pd.isna(candidate) else candidate


def list_airlines(data_store, query: Optional[str]) -> List[Dict[str, Any]]:
    """Return cached airline metadata filtered by an optional substring."""
    airlines_df = data_store.airlines.copy()
    if query:
        mask = (
            airlines_df["Airline"].str.contains(query, case=False, na=False)
            | airlines_df["Alias"].str.contains(query, case=False, na=False)
            | airlines_df["IATA"].str.contains(query, case=False, na=False)
        )
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


def route_analysis(data_store, payload: RouteAnalysisRequest) -> Dict[str, Any]:
    """Run a lightweight route-focused analysis for a single airline."""
    if not payload.airline.strip():
        raise AnalysisError(400, "Airline name is required.")

    try:
        routes_df, metadata = data_store.select_airline_routes(payload.airline)
    except ValueError as exc:
        raise AnalysisError(404, str(exc)) from exc

    if routes_df.empty:
        raise AnalysisError(404, "No routes found for the requested airline.")

    processed = data_store.process_routes(routes_df)
    processed["Total"] = pd.to_numeric(processed.get("Total"), errors="coerce").fillna(0.0)
    processed["Distance (miles)"] = pd.to_numeric(
        processed.get("Distance (miles)"), errors="coerce"
    ).fillna(0.0)
    processed["ASM"] = processed["Total"] * processed["Distance (miles)"]
    processed["ASM"] = processed["ASM"].fillna(0.0)

    total_routes = int(len(processed))
    summary = {
        "total_routes": total_routes,
        "total_seats": float(processed["Total"].sum()),
        "average_distance": _safe_number(processed["Distance (miles)"].mean()),
        "longest_route": _safe_number(processed["Distance (miles)"].max()),
    }

    network = data_store.build_network(processed)
    network_stats = data_store.analyze_network(
        network,
        metadata.get("Airline (Normalized)") if metadata is not None else None,
        processed_routes=processed,
    )

    airline_code = (metadata.get("IATA") if metadata is not None else "") or ""
    airline_code = str(airline_code).strip().upper()
    equipment_counts = {}
    if airline_code and hasattr(data_store, "top_equipment_flights_per_day"):
        equipment_counts = data_store.top_equipment_flights_per_day(airline_code, top_n=5)
    elif airline_code and hasattr(data_store, "top_equipment_by_departures"):
        equipment_counts = data_store.top_equipment_by_departures(airline_code, top_n=5)
    if not equipment_counts:
        equipment_counts = (
            processed["Equipment Display" if "Equipment Display" in processed.columns else "Equipment"]
            .fillna("Unknown")
            .astype(str)
            .str.strip()
            .replace({"": "Unknown"})
            .value_counts()
            .head(5)
            .to_dict()
        )

    route_columns = [
        "Source airport Display",
        "Destination airport Display",
        "Equipment Display",
        "Distance (miles)",
        "Total",
        "ASM",
    ]

    if {"Source Country", "Destination Country"}.issubset(processed.columns):
        source_country = processed["Source Country"].fillna("").astype(str).str.strip()
        destination_country = processed["Destination Country"].fillna("").astype(str).str.strip()
        international_mask = (
            source_country.ne("")
            & destination_country.ne("")
            & source_country.ne(destination_country)
        )
    else:
        international_mask = pd.Series(False, index=processed.index)

    top_routes = (
        processed.loc[~international_mask]
        .sort_values("ASM", ascending=False)
        .head(payload.limit)
        .loc[:, route_columns]
        .copy()
    )
    top_international_routes = (
        processed.loc[international_mask]
        .sort_values("ASM", ascending=False)
        .head(payload.limit)
        .loc[:, route_columns]
        .copy()
    )

    return {
        "airline": _format_airline_metadata(_normalize_metadata(metadata)),
        "summary": summary,
        "network": network_stats,
        "top_equipment": equipment_counts,
        "top_routes": _dataframe_to_records(top_routes),
        "top_international_routes": _dataframe_to_records(top_international_routes),
    }


def get_airline_fleet_profile(data_store, airline_query: str, limit: int = 20) -> Dict[str, Any]:
    """Legacy helper retained for test coverage and compatibility."""
    request = RouteAnalysisRequest(airline=str(airline_query), limit=int(limit))
    result = route_analysis(data_store, request)

    airline = result.get("airline") or {}
    metadata_total_routes = (result.get("summary") or {}).get("total_routes")
    if isinstance(metadata_total_routes, (int, float)):
        airline["total_routes"] = int(metadata_total_routes)

    return {
        "airline": airline,
        "network_stats": result.get("network") or {},
        "top_routes": result.get("top_routes") or [],
    }
