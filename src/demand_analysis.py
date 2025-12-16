from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd


NYC_AIRPORTS = {"JFK", "LGA", "EWR", "HPN", "ISP", "SWF"}
LA_AIRPORTS = {"LAX", "BUR", "SNA", "LGB", "ONT"}
CHI_AIRPORTS = {"ORD", "MDW"}
BIG3_AIRPORTS = NYC_AIRPORTS | LA_AIRPORTS | CHI_AIRPORTS


@dataclass(frozen=True)
class ConcentrationStats:
    markets: int
    total_passengers: float
    top_share: float
    top_markets: int
    top_passengers: float
    long_tail_markets: int
    long_tail_passengers: float

    @property
    def top_passenger_share(self) -> float:
        if self.total_passengers <= 0:
            return float("nan")
        return self.top_passengers / self.total_passengers

    @property
    def long_tail_passenger_share(self) -> float:
        if self.total_passengers <= 0:
            return float("nan")
        return self.long_tail_passengers / self.total_passengers


def _require_columns(df: pd.DataFrame, required: Iterable[str]) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def _normalize_airport_codes(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def load_domestic_demand_mart(path: str | Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_csv(path)
    _require_columns(df, ["year", "quarter", "origin", "dest", "passengers", "avg_fare", "distance"])
    return df


def build_market_quarterly(
    demand_mart: pd.DataFrame,
    *,
    directional: bool = False,
    since_year: int | None = 2022,
    origin_col: str = "origin",
    dest_col: str = "dest",
) -> pd.DataFrame:
    _require_columns(demand_mart, ["year", "quarter", origin_col, dest_col, "passengers", "avg_fare", "distance"])

    df = demand_mart.copy()
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce").astype("Int64")
    df = df[df["year"].notna() & df["quarter"].between(1, 4)].copy()
    if since_year is not None:
        df = df[df["year"] >= int(since_year)].copy()

    df[origin_col] = _normalize_airport_codes(df[origin_col])
    df[dest_col] = _normalize_airport_codes(df[dest_col])

    for col in ("passengers", "avg_fare", "distance"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df = df[(df[origin_col] != "") & (df[dest_col] != "")].copy()

    revenue_proxy = df["passengers"] * df["avg_fare"]
    distance_x_pax = df["passengers"] * df["distance"]

    origin_values = df[origin_col].to_numpy(dtype=str)
    dest_values = df[dest_col].to_numpy(dtype=str)

    if directional:
        market_a = origin_values
        market_b = dest_values
    else:
        swap_mask = origin_values > dest_values
        market_a = np.where(swap_mask, dest_values, origin_values)
        market_b = np.where(swap_mask, origin_values, dest_values)

    df["market_a"] = market_a
    df["market_b"] = market_b
    df["revenue_proxy"] = revenue_proxy
    df["distance_x_pax"] = distance_x_pax

    grouped = (
        df.groupby(["year", "quarter", "market_a", "market_b"], as_index=False)
        .agg(
            passengers=("passengers", "sum"),
            revenue_proxy=("revenue_proxy", "sum"),
            distance_x_pax=("distance_x_pax", "sum"),
        )
        .reset_index(drop=True)
    )

    denom = grouped["passengers"].where(grouped["passengers"] > 0, pd.NA)
    grouped["avg_fare"] = grouped["revenue_proxy"] / denom
    grouped["distance"] = grouped["distance_x_pax"] / denom
    grouped["fare_per_mile"] = grouped["avg_fare"] / grouped["distance"].where(grouped["distance"] > 0, pd.NA)
    grouped["market"] = grouped["market_a"].astype(str) + "-" + grouped["market_b"].astype(str)

    return grouped.drop(columns=["revenue_proxy", "distance_x_pax"]).sort_values(
        ["year", "quarter", "market_a", "market_b"],
        kind="mergesort",
    ).reset_index(drop=True)


def aggregate_market_totals(market_quarterly: pd.DataFrame) -> pd.DataFrame:
    _require_columns(market_quarterly, ["market_a", "market_b", "passengers", "avg_fare", "distance"])

    df = market_quarterly.copy()
    for col in ("passengers", "avg_fare", "distance"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["revenue_proxy"] = df["passengers"] * df["avg_fare"]
    df["distance_x_pax"] = df["passengers"] * df["distance"]

    grouped = (
        df.groupby(["market_a", "market_b"], as_index=False)
        .agg(
            passengers_total=("passengers", "sum"),
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


def rank_markets(
    market_totals: pd.DataFrame,
    *,
    top_n: int = 50,
    exclude_airports: Optional[set[str]] = None,
) -> pd.DataFrame:
    _require_columns(market_totals, ["market_a", "market_b", "passengers_total"])

    df = market_totals.copy()
    if exclude_airports:
        exclude = {str(code).strip().upper() for code in exclude_airports if code}
        df = df[~df["market_a"].isin(exclude) & ~df["market_b"].isin(exclude)].copy()

    df = df.sort_values(["passengers_total", "market_a", "market_b"], ascending=[False, True, True], kind="mergesort")
    df = df.head(max(0, int(top_n))).reset_index(drop=True)
    df.insert(0, "rank", np.arange(1, len(df) + 1, dtype=int))
    return df


def compute_concentration(
    market_totals: pd.DataFrame,
    *,
    top_share: float = 0.10,
    passenger_col: str = "passengers_total",
) -> ConcentrationStats:
    if not (0 < float(top_share) < 1):
        raise ValueError("top_share must be between 0 and 1.")
    _require_columns(market_totals, [passenger_col])

    passengers = pd.to_numeric(market_totals[passenger_col], errors="coerce").fillna(0.0)
    total_passengers = float(passengers.sum())
    markets = int(len(passengers))
    if markets == 0:
        return ConcentrationStats(
            markets=0,
            total_passengers=0.0,
            top_share=float(top_share),
            top_markets=0,
            top_passengers=0.0,
            long_tail_markets=0,
            long_tail_passengers=0.0,
        )

    sorted_passengers = passengers.sort_values(ascending=False, kind="mergesort").reset_index(drop=True)
    top_markets = int(np.ceil(markets * float(top_share)))
    top_markets = max(1, min(markets, top_markets))

    top_passengers = float(sorted_passengers.iloc[:top_markets].sum())
    long_tail_passengers = float(sorted_passengers.iloc[top_markets:].sum())
    return ConcentrationStats(
        markets=markets,
        total_passengers=total_passengers,
        top_share=float(top_share),
        top_markets=top_markets,
        top_passengers=top_passengers,
        long_tail_markets=markets - top_markets,
        long_tail_passengers=long_tail_passengers,
    )


def market_stability_analysis(
    market_quarterly: pd.DataFrame,
    *,
    stable_residual_cv_max: float = 0.25,
    stable_seasonality_max: float = 0.40,
    seasonal_seasonality_min: float = 0.60,
    seasonal_residual_cv_max: float = 0.35,
) -> pd.DataFrame:
    _require_columns(market_quarterly, ["year", "quarter", "market_a", "market_b", "passengers"])

    base = market_quarterly[["year", "quarter", "market_a", "market_b", "passengers"]].copy()
    base["year"] = pd.to_numeric(base["year"], errors="coerce").astype(int)
    base["quarter"] = pd.to_numeric(base["quarter"], errors="coerce").astype(int)
    base["passengers"] = pd.to_numeric(base["passengers"], errors="coerce").fillna(0.0)

    periods = base[["year", "quarter"]].drop_duplicates().sort_values(["year", "quarter"]).reset_index(drop=True)
    markets = base[["market_a", "market_b"]].drop_duplicates().reset_index(drop=True)

    if periods.empty or markets.empty:
        return pd.DataFrame(
            columns=[
                "market_a",
                "market_b",
                "market",
                "total_passengers",
                "mean_passengers",
                "std_passengers",
                "cv",
                "seasonality_index",
                "residual_cv",
                "n_quarters",
                "active_quarters",
                "active_share",
                "classification",
            ]
        )

    periods = periods.assign(_key=1)
    markets = markets.assign(_key=1)
    panel = markets.merge(periods, on="_key").drop(columns=["_key"])
    panel = panel.merge(base, on=["market_a", "market_b", "year", "quarter"], how="left")
    panel["passengers"] = panel["passengers"].fillna(0.0)

    n_quarters = int(len(periods))

    std_passengers = panel.groupby(["market_a", "market_b"])["passengers"].std(ddof=0)
    mean_passengers = panel.groupby(["market_a", "market_b"])["passengers"].mean()
    total_passengers = panel.groupby(["market_a", "market_b"])["passengers"].sum()
    active_quarters = panel.groupby(["market_a", "market_b"])["passengers"].apply(lambda s: int((s > 0).sum()))

    quarter_means = (
        panel.groupby(["market_a", "market_b", "quarter"], as_index=False)
        .agg(seasonal_mean=("passengers", "mean"))
        .reset_index(drop=True)
    )
    season_bounds = quarter_means.groupby(["market_a", "market_b"])["seasonal_mean"].agg(["min", "max"])
    seasonality_index = (season_bounds["max"] - season_bounds["min"]) / mean_passengers.replace(0.0, np.nan)

    panel = panel.merge(quarter_means, on=["market_a", "market_b", "quarter"], how="left")
    panel["seasonal_mean"] = panel["seasonal_mean"].fillna(0.0)
    panel["residual"] = panel["passengers"] - panel["seasonal_mean"]
    residual_std = panel.groupby(["market_a", "market_b"])["residual"].std(ddof=0)
    residual_cv = residual_std / mean_passengers.replace(0.0, np.nan)

    stats = pd.DataFrame(
        {
            "total_passengers": total_passengers,
            "mean_passengers": mean_passengers,
            "std_passengers": std_passengers,
            "cv": std_passengers / mean_passengers.replace(0.0, np.nan),
            "seasonality_index": seasonality_index,
            "residual_cv": residual_cv,
            "n_quarters": n_quarters,
            "active_quarters": active_quarters,
        }
    ).reset_index()
    stats["active_share"] = stats["active_quarters"] / float(n_quarters) if n_quarters > 0 else 0.0
    stats["market"] = stats["market_a"].astype(str) + "-" + stats["market_b"].astype(str)

    def _classify(row: pd.Series) -> str:
        mean_val = row.get("mean_passengers")
        if mean_val is None or pd.isna(mean_val) or float(mean_val) <= 0:
            return "No demand"
        seasonality = row.get("seasonality_index")
        residual = row.get("residual_cv")
        if pd.notna(residual) and pd.notna(seasonality):
            if float(residual) <= stable_residual_cv_max and float(seasonality) <= stable_seasonality_max:
                return "Stable core"
            if float(seasonality) >= seasonal_seasonality_min and float(residual) <= seasonal_residual_cv_max:
                return "Seasonal leisure"
        return "Volatile / emerging"

    stats["classification"] = stats.apply(_classify, axis=1)
    return stats.sort_values(["total_passengers", "market_a", "market_b"], ascending=[False, True, True], kind="mergesort").reset_index(
        drop=True
    )

