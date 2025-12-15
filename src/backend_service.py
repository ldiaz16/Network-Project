"""Minimal backend helpers focused on T-100 route analysis."""

from typing import Any, Dict, List, Optional

import pandas as pd
from data.airlines import normalize_name
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


class AllianceAnalysisRequest(BaseModel):
    carrier_group: int = Field(..., description="Carrier group code (BTS CARRIER_GROUP).")
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


def list_alliances(data_store) -> List[Dict[str, Any]]:
    """List available carrier groups present in the loaded routes dataset."""
    routes_df = getattr(data_store, "routes", None)
    if not isinstance(routes_df, pd.DataFrame) or routes_df.empty:
        return []
    if "Carrier Group" not in routes_df.columns:
        return []

    codes = pd.to_numeric(routes_df["Carrier Group"], errors="coerce").dropna().astype(int)
    if codes.empty:
        return []

    lookup = getattr(data_store, "carrier_group_lookup", None) or {}
    return [
        {"code": int(code), "name": lookup.get(int(code), f"Carrier Group {int(code)}")}
        for code in sorted(codes.unique().tolist())
    ]


def alliance_analysis(data_store, payload: AllianceAnalysisRequest) -> Dict[str, Any]:
    """Aggregate an alliance (carrier group) network across all member airlines."""
    routes_df = getattr(data_store, "routes", None)
    if not isinstance(routes_df, pd.DataFrame) or routes_df.empty:
        raise AnalysisError(503, "Routes data is not loaded.")

    if "Carrier Group" not in routes_df.columns:
        raise AnalysisError(
            400,
            "Carrier group data is not available. Provide `T_T100_SEGMENT_ALL_CARRIER-2.csv` alongside the T-100 export.",
        )

    carrier_group = int(payload.carrier_group)
    group_values = pd.to_numeric(routes_df["Carrier Group"], errors="coerce")
    group_routes = routes_df.loc[group_values.eq(carrier_group)].copy()
    if group_routes.empty:
        raise AnalysisError(404, "No routes found for the requested alliance.")

    lookup = getattr(data_store, "carrier_group_lookup", None) or {}
    group_name = lookup.get(carrier_group, f"Carrier Group {carrier_group}")
    normalized_group = normalize_name(group_name)

    def _most_common(series: pd.Series) -> Optional[str]:
        cleaned = series.dropna().astype(str)
        if cleaned.empty:
            return None
        mode = cleaned.mode()
        return mode.iloc[0] if not mode.empty else None

    agg: Dict[str, Any] = {
        "Total": ("Total", "sum"),
    }
    if "Passengers" in group_routes.columns:
        agg["Passengers"] = ("Passengers", "sum")
    if "Distance (reported)" in group_routes.columns:
        agg["Distance (reported)"] = ("Distance (reported)", "mean")
    for optional in (
        "Equipment",
        "Equipment Code",
        "Equipment Description",
        "Source city",
        "Destination city",
        "Seat Source",
    ):
        if optional in group_routes.columns:
            agg[optional] = (optional, _most_common)

    aggregated = (
        group_routes.groupby(["Source airport", "Destination airport"], dropna=False)
        .agg(**agg)
        .reset_index()
    )
    aggregated["Airline"] = group_name
    aggregated["Airline (Normalized)"] = normalized_group
    if "Seat Source" not in aggregated.columns:
        aggregated["Seat Source"] = "t100_segments"

    processed = data_store.process_routes(aggregated)
    processed["Total"] = pd.to_numeric(processed.get("Total"), errors="coerce").fillna(0.0)
    processed["Distance (miles)"] = pd.to_numeric(processed.get("Distance (miles)"), errors="coerce").fillna(0.0)
    processed["ASM"] = processed["Total"] * processed["Distance (miles)"]
    processed["ASM"] = processed["ASM"].fillna(0.0)

    summary = {
        "member_carriers": int(group_routes["Airline Code"].nunique()) if "Airline Code" in group_routes.columns else 0,
        "total_routes": int(len(processed)),
        "total_seats": float(processed["Total"].sum()),
        "average_distance": _safe_number(processed["Distance (miles)"].mean()),
        "longest_route": _safe_number(processed["Distance (miles)"].max()),
    }

    network = data_store.build_network(processed)
    network_stats = data_store.analyze_network(network, airline_identifier=normalized_group, processed_routes=processed)

    carrier_rows: List[Dict[str, Any]] = []
    if "Airline Code" in group_routes.columns:
        carrier_summary = (
            group_routes.groupby("Airline Code", dropna=False)
            .agg(total_seats=("Total", "sum"), total_routes=("Total", "size"))
            .reset_index()
            .sort_values("total_seats", ascending=False)
            .head(15)
        )
        airlines_df = getattr(data_store, "airlines", None)
        name_map: Dict[str, str] = {}
        country_map: Dict[str, str] = {}
        if isinstance(airlines_df, pd.DataFrame) and not airlines_df.empty:
            if {"IATA", "Airline"}.issubset(airlines_df.columns):
                name_map = (
                    airlines_df[["IATA", "Airline"]]
                    .dropna()
                    .astype(str)
                    .assign(IATA=lambda df: df["IATA"].str.strip().str.upper())
                    .drop_duplicates(subset=["IATA"])
                    .set_index("IATA")["Airline"]
                    .to_dict()
                )
            if {"IATA", "Country"}.issubset(airlines_df.columns):
                country_map = (
                    airlines_df[["IATA", "Country"]]
                    .dropna()
                    .astype(str)
                    .assign(IATA=lambda df: df["IATA"].str.strip().str.upper())
                    .drop_duplicates(subset=["IATA"])
                    .set_index("IATA")["Country"]
                    .to_dict()
                )

        carrier_rows = [
            {
                "iata": str(row["Airline Code"]).strip().upper() if pd.notna(row["Airline Code"]) else None,
                "airline": name_map.get(str(row["Airline Code"]).strip().upper(), None),
                "country": country_map.get(str(row["Airline Code"]).strip().upper(), None),
                "total_seats": _safe_number(row.get("total_seats")),
                "total_routes": int(_safe_number(row.get("total_routes"))),
            }
            for _, row in carrier_summary.iterrows()
        ]

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
        "alliance": {"code": carrier_group, "name": group_name},
        "summary": summary,
        "network": network_stats,
        "top_carriers": carrier_rows,
        "top_routes": _dataframe_to_records(top_routes),
        "top_international_routes": _dataframe_to_records(top_international_routes),
    }


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
