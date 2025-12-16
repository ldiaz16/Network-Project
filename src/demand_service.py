from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from src.demand_analysis import (
    BIG3_AIRPORTS,
    ConcentrationStats,
    aggregate_market_totals,
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
    market_quarterly: Dict[Tuple[int, bool, int], pd.DataFrame] = field(default_factory=dict)
    market_totals: Dict[Tuple[int, bool, int], pd.DataFrame] = field(default_factory=dict)
    stability: Dict[Tuple[int, bool, int], pd.DataFrame] = field(default_factory=dict)

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
            self.market_quarterly.clear()
            self.market_totals.clear()
            self.stability.clear()
        return self.mart

    def get_market_quarterly(self, *, since_year: int, directional: bool, mart_path: Optional[Path] = None) -> pd.DataFrame:
        mart = self.get_mart(mart_path=mart_path)
        key = (int(since_year), bool(directional), int(self.mart_mtime_ns))
        cached = self.market_quarterly.get(key)
        if cached is not None:
            return cached
        built = build_market_quarterly(mart, directional=directional, since_year=since_year)
        self.market_quarterly[key] = built
        return built

    def get_market_totals(self, *, since_year: int, directional: bool, mart_path: Optional[Path] = None) -> pd.DataFrame:
        key = (int(since_year), bool(directional), int(self.mart_mtime_ns))
        cached = self.market_totals.get(key)
        if cached is not None:
            return cached
        quarterly = self.get_market_quarterly(since_year=since_year, directional=directional, mart_path=mart_path)
        totals = aggregate_market_totals(quarterly)
        self.market_totals[key] = totals
        return totals

    def get_stability(self, *, since_year: int, directional: bool, mart_path: Optional[Path] = None) -> pd.DataFrame:
        key = (int(since_year), bool(directional), int(self.mart_mtime_ns))
        cached = self.stability.get(key)
        if cached is not None:
            return cached
        quarterly = self.get_market_quarterly(since_year=since_year, directional=directional, mart_path=mart_path)
        stats = market_stability_analysis(quarterly)
        self.stability[key] = stats
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
