from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from src.demand_analysis import (
    BIG3_AIRPORTS,
    ConcentrationStats,
    build_market_quarterly,
    compute_concentration,
    load_domestic_demand_mart,
    market_stability_analysis,
    rank_markets,
)


def _default_mart_path(base_dir: Path) -> Optional[Path]:
    candidates = [
        base_dir / "datasets" / "db1b" / "processed" / "domestic_demand_mart.parquet",
        base_dir / "datasets" / "db1b" / "processed" / "domestic_demand_mart.csv",
        base_dir / "data" / "domestic_demand_mart.parquet",
        base_dir / "data" / "domestic_demand_mart.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _stat_mtime_ns(path: Path) -> int:
    return int(path.stat().st_mtime_ns)


@dataclass
class DemandMartCache:
    base_dir: Path
    mart_path: Optional[Path] = None
    mart_mtime_ns: int = 0
    mart: Optional[pd.DataFrame] = None
    market_totals: Dict[Tuple[int, bool, int], pd.DataFrame] = field(default_factory=dict)
    stability: Dict[Tuple[int, bool, int], pd.DataFrame] = field(default_factory=dict)
    max_cache_entries: int = 4

    def _resolve_mart_path(self, override: Optional[Path] = None) -> Path:
        if override is not None:
            path = Path(override)
            if not path.exists():
                raise FileNotFoundError(path)
            return path

        if self.mart_path and self.mart_path.exists():
            return self.mart_path

        path = _default_mart_path(self.base_dir)
        if path is None:
            raise FileNotFoundError(
                "Domestic demand mart not found. Build it with "
                "`python3 scripts/build_domestic_demand_mart.py`."
            )
        self.mart_path = path
        return path

    def get_mart(self, *, mart_path: Optional[Path] = None) -> pd.DataFrame:
        path = self._resolve_mart_path(mart_path)
        mtime = _stat_mtime_ns(path)
        if self.mart is None or mtime != self.mart_mtime_ns:
            self.mart = load_domestic_demand_mart(path)
            self.mart_mtime_ns = mtime
            self.market_totals.clear()
            self.stability.clear()
        return self.mart

    def _prune_cache(self, cache: Dict[Tuple[int, bool, int], pd.DataFrame]) -> None:
        limit = max(0, int(self.max_cache_entries))
        if limit == 0:
            cache.clear()
            return
        while len(cache) > limit:
            cache.pop(next(iter(cache)))

    def _build_market_totals(self, mart: pd.DataFrame, *, since_year: int, directional: bool) -> pd.DataFrame:
        required = {"year", "origin", "dest", "passengers", "avg_fare", "distance"}
        missing = sorted(required - set(mart.columns))
        if missing:
            raise ValueError(f"Domestic demand mart missing required columns: {', '.join(missing)}")

        years = pd.to_numeric(mart["year"], errors="coerce")
        mask = years.notna() & (years >= int(since_year))

        origin = mart["origin"].fillna("").astype(str).str.strip().str.upper()
        dest = mart["dest"].fillna("").astype(str).str.strip().str.upper()
        mask &= origin.ne("") & dest.ne("")

        passengers = pd.to_numeric(mart["passengers"], errors="coerce").fillna(0.0)
        avg_fare = pd.to_numeric(mart["avg_fare"], errors="coerce").fillna(0.0)
        distance = pd.to_numeric(mart["distance"], errors="coerce").fillna(0.0)

        origin_values = origin[mask].to_numpy(dtype=str)
        dest_values = dest[mask].to_numpy(dtype=str)

        if directional:
            market_a = origin_values
            market_b = dest_values
        else:
            swap = origin_values > dest_values
            market_a = np.where(swap, dest_values, origin_values)
            market_b = np.where(swap, origin_values, dest_values)

        pax = passengers[mask].to_numpy(dtype=float)
        revenue_proxy = pax * avg_fare[mask].to_numpy(dtype=float)
        distance_x_pax = pax * distance[mask].to_numpy(dtype=float)

        base = pd.DataFrame(
            {
                "market_a": market_a,
                "market_b": market_b,
                "passengers_total": pax,
                "revenue_proxy": revenue_proxy,
                "distance_x_pax": distance_x_pax,
            }
        )

        grouped = (
            base.groupby(["market_a", "market_b"], as_index=False)
            .agg(
                passengers_total=("passengers_total", "sum"),
                revenue_proxy=("revenue_proxy", "sum"),
                distance_x_pax=("distance_x_pax", "sum"),
            )
            .reset_index(drop=True)
        )

        denom = grouped["passengers_total"].where(grouped["passengers_total"] > 0, pd.NA)
        grouped["avg_fare"] = grouped["revenue_proxy"] / denom
        grouped["distance"] = grouped["distance_x_pax"] / denom
        grouped["fare_per_mile"] = grouped["avg_fare"] / grouped["distance"].where(grouped["distance"] > 0, pd.NA)
        grouped["market"] = grouped["market_a"].astype(str) + "-" + grouped["market_b"].astype(str)
        return grouped.drop(columns=["revenue_proxy", "distance_x_pax"]).sort_values(
            ["passengers_total", "market_a", "market_b"],
            ascending=[False, True, True],
            kind="mergesort",
        ).reset_index(drop=True)

    def get_market_totals(self, *, since_year: int, directional: bool, mart_path: Optional[Path] = None) -> pd.DataFrame:
        mart = self.get_mart(mart_path=mart_path)
        key = (int(since_year), bool(directional), int(self.mart_mtime_ns))
        cached = self.market_totals.get(key)
        if cached is not None:
            return cached
        totals = self._build_market_totals(mart, since_year=since_year, directional=directional)
        self.market_totals[key] = totals
        self._prune_cache(self.market_totals)
        return totals

    def get_stability(self, *, since_year: int, directional: bool, mart_path: Optional[Path] = None) -> pd.DataFrame:
        mart = self.get_mart(mart_path=mart_path)
        key = (int(since_year), bool(directional), int(self.mart_mtime_ns))
        cached = self.stability.get(key)
        if cached is not None:
            return cached
        quarterly = build_market_quarterly(mart, directional=directional, since_year=since_year)
        stats = market_stability_analysis(quarterly)
        self.stability[key] = stats
        self._prune_cache(self.stability)
        return stats


def get_top_markets(
    cache: DemandMartCache,
    *,
    since_year: int = 2022,
    directional: bool = False,
    top_n: int = 50,
    exclude_big3: bool = False,
    mart_path: Optional[Path] = None,
) -> pd.DataFrame:
    totals = cache.get_market_totals(since_year=since_year, directional=directional, mart_path=mart_path)
    exclude = BIG3_AIRPORTS if exclude_big3 else None
    return rank_markets(totals, top_n=top_n, exclude_airports=exclude)


def get_concentration_summary(
    cache: DemandMartCache,
    *,
    since_year: int = 2022,
    directional: bool = False,
    mart_path: Optional[Path] = None,
    top_share: float = 0.10,
) -> ConcentrationStats:
    totals = cache.get_market_totals(since_year=since_year, directional=directional, mart_path=mart_path)
    return compute_concentration(totals, top_share=top_share)


def get_stability_page(
    cache: DemandMartCache,
    *,
    since_year: int = 2022,
    directional: bool = False,
    mart_path: Optional[Path] = None,
    q: str | None = None,
    classification: str | None = None,
    min_total_passengers: float = 0.0,
    sort_by: str = "total_passengers",
    sort_dir: str = "desc",
    offset: int = 0,
    limit: int = 500,
) -> Dict[str, Any]:
    df = cache.get_stability(since_year=since_year, directional=directional, mart_path=mart_path)
    filtered = df.copy()

    if q:
        token = str(q).strip().upper()
        if token:
            filtered = filtered[
                filtered["market_a"].astype(str).str.contains(token, na=False)
                | filtered["market_b"].astype(str).str.contains(token, na=False)
            ].copy()

    if classification:
        label = str(classification).strip()
        if label:
            filtered = filtered[filtered["classification"] == label].copy()

    if min_total_passengers:
        filtered = filtered[pd.to_numeric(filtered["total_passengers"], errors="coerce").fillna(0.0) >= float(min_total_passengers)].copy()

    class_counts = (
        filtered["classification"].value_counts(dropna=False).to_dict()
        if "classification" in filtered.columns
        else {}
    )

    allowed_sorts = {
        "total_passengers",
        "mean_passengers",
        "std_passengers",
        "cv",
        "seasonality_index",
        "residual_cv",
        "active_share",
        "market",
    }
    sort_key = sort_by if sort_by in allowed_sorts else "total_passengers"
    ascending = str(sort_dir).lower().startswith("asc")
    if sort_key in filtered.columns:
        filtered = filtered.sort_values([sort_key, "market_a", "market_b"], ascending=[ascending, True, True], kind="mergesort")

    total = int(len(filtered))
    safe_offset = max(0, int(offset))
    safe_limit = max(1, min(10_000, int(limit)))
    page = filtered.iloc[safe_offset : safe_offset + safe_limit].copy()

    return {
        "total": total,
        "offset": safe_offset,
        "limit": safe_limit,
        "classification_counts": class_counts,
        "rows": page.to_dict(orient="records"),
    }
