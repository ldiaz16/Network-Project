"""
Backtest monthly P/L forecasts using rolling window.

Models:
  - OLS (same as pnl_forecast.py spec)
  - ARIMAX (same as pnl_forecast_arimax.py spec)

Data: data/us_passenger_monthly.parquet

Outputs:
  reports/backtest_monthly.csv
"""

import pathlib
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.statespace.sarimax import SARIMAX

DATA_PATH = pathlib.Path("data/us_passenger_monthly_with_yield.parquet")
OUT_PATH = pathlib.Path("reports/backtest_monthly.csv")


def build_features(df: pd.DataFrame):
    df = df.copy()
    # Normalize target by ASM
    if "ASM" in df.columns:
        df["pl_per_asm"] = df["Operating_PL"] / df["ASM"]
    else:
        df["pl_per_asm"] = df["Operating_PL"]
    df["trend"] = np.arange(len(df))
    df["month"] = df["date"].dt.month
    df["m_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["m_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    df["p_lag1"] = df["Operating_PL"].shift(1)
    df["p_lag2"] = df["Operating_PL"].shift(2)
    df["rev_lag1"] = df["Operating_Revenue"].shift(1)
    df["rev_lag2"] = df["Operating_Revenue"].shift(2)
    feature_cols = ["trend", "m_sin", "m_cos", "p_lag1", "p_lag2", "rev_lag1", "rev_lag2", "Operating_Revenue"]
    for col in ("Passengers", "ASM", "RPM", "Flights", "LoadFactor", "jet_fuel_usgc", "yield_rev_per_rpm", "yield_rev_per_pax", "rev_per_asm"):
        if col in df.columns:
            feature_cols.append(col)
    df[feature_cols] = df[feature_cols].apply(pd.to_numeric, errors="coerce")
    df[feature_cols] = df[feature_cols].ffill().bfill().fillna(0)
    df["Operating_PL"] = pd.to_numeric(df["Operating_PL"], errors="coerce")
    df = df.dropna(subset=["Operating_PL"])
    return df, feature_cols


def seasonal_growth_forecast(
    series: pd.Series, horizon: int, season: int = 12, lookback: int = 24, cap: float = 0.10
):
    """Seasonal pattern with bounded median pct growth to avoid runaway drift."""
    vals = series.to_numpy()
    if len(vals) == 0:
        return np.zeros(horizon)
    season_base = vals[-season:] if len(vals) >= season else np.resize(vals, season)
    pct_changes = pd.Series(vals).pct_change().dropna()
    if len(pct_changes) > 0:
        g = pct_changes.tail(lookback).median()
        g = float(np.clip(g, -cap, cap))
    else:
        g = 0.0
    forecasts = []
    for i in range(horizon):
        season_val = season_base[i % season]
        grow_factor = (1 + g) ** ((i + 1) / float(season))
        forecasts.append(season_val * grow_factor)
    return np.array(forecasts)


def forecast_ols(train_df: pd.DataFrame, feature_cols, horizon: int):
    X = train_df[feature_cols]
    y = train_df["pl_per_asm"]
    model = LinearRegression().fit(X, y)
    preds = []
    last = train_df.iloc[-1]
    # Prepare history for lags
    p_hist = train_df["pl_per_asm"].tolist()
    rev_hist = train_df["Operating_Revenue"].tolist()
    asm_hist = train_df["ASM"].tolist() if "ASM" in train_df else [1.0] * len(train_df)
    asm_fc = seasonal_growth_forecast(train_df["ASM"], horizon, cap=0.10) if "ASM" in train_df else np.ones(horizon)
    for i in range(horizon):
        quarter = last["date"] + pd.offsets.MonthBegin(i + 1)
        month = quarter.month
        feats = {
            "trend": len(train_df) + i,
            "m_sin": np.sin(2 * np.pi * month / 12),
            "m_cos": np.cos(2 * np.pi * month / 12),
            "p_lag1": p_hist[-1] if i == 0 else preds[-1],
            "p_lag2": p_hist[-2] if i == 0 else (p_hist[-1] if i == 1 else preds[-2]),
            "rev_lag1": rev_hist[-1],
            "rev_lag2": rev_hist[-2] if len(rev_hist) >= 2 else rev_hist[-1],
            "Operating_Revenue": rev_hist[-1],  # flat
        }
        for col in feature_cols:
            if col not in feats:
                feats[col] = last.get(col, 0)
        feats_df = pd.DataFrame([feats])[feature_cols]
        pred = model.predict(feats_df)[0]
        preds.append(pred)
    # Convert per-ASM preds back to dollars using ASM forecast
    preds_dollars = np.array(preds) * asm_fc
    return preds_dollars


def forecast_arimax(train_df: pd.DataFrame, horizon: int):
    train_df = train_df.set_index("date")
    y = pd.to_numeric(train_df["pl_per_asm"], errors="coerce")
    exog_cols = [c for c in train_df.columns if c != "Operating_PL"]
    exog_hist = train_df[exog_cols].apply(pd.to_numeric, errors="coerce").ffill().bfill()

    # Shock dummy on large P/L jumps
    shocks = pd.Series(0, index=train_df.index, dtype=float)
    diffs = y.diff()
    threshold = diffs.abs().quantile(0.95)
    shocks.loc[diffs.abs() >= threshold] = 1.0
    # Manual event windows (GFC and COVID onset)
    gfc_start, gfc_end = pd.to_datetime("2008-09-01"), pd.to_datetime("2009-06-30")
    covid_start, covid_end = pd.to_datetime("2020-03-01"), pd.to_datetime("2020-09-30")
    shocks.loc[(shocks.index >= gfc_start) & (shocks.index <= gfc_end)] = 1.0
    shocks.loc[(shocks.index >= covid_start) & (shocks.index <= covid_end)] = 1.0
    exog_hist = exog_hist.assign(shock=shocks)
    if "shock" not in exog_cols:
        exog_cols.append("shock")

    model = SARIMAX(
        y,
        exog=exog_hist,
        order=(1, 0, 1),
        seasonal_order=(1, 0, 1, 12),
        enforce_stationarity=False,
        enforce_invertibility=False,
        trend="c",
    )
    res = model.fit(disp=False, method="powell", maxiter=75)

    # Build future exog with seasonal growth for revenue/fuel/ASM, shocks set to 0
    last_exog = exog_hist.iloc[-1]
    rev_fc = (
        seasonal_growth_forecast(exog_hist["Operating_Revenue"], horizon, cap=0.10)
        if "Operating_Revenue" in exog_hist
        else None
    )
    fuel_fc = (
        seasonal_growth_forecast(exog_hist["jet_fuel_usgc"], horizon, lookback=12, cap=0.10)
        if "jet_fuel_usgc" in exog_hist
        else None
    )
    asm_fc = (
        seasonal_growth_forecast(exog_hist["ASM"], horizon, lookback=24, cap=0.10)
        if "ASM" in exog_hist
        else np.ones(horizon)
    )
    future_rows = []
    for i in range(horizon):
        row = last_exog.copy()
        row["shock"] = 0.0
        if rev_fc is not None:
            row["Operating_Revenue"] = rev_fc[i]
        if fuel_fc is not None:
            row["jet_fuel_usgc"] = fuel_fc[i]
        if "ASM" in exog_hist:
            row["ASM"] = asm_fc[i]
        future_rows.append(row)
    exog_future = pd.DataFrame(future_rows, columns=exog_cols)

    fcst_per_asm = res.get_forecast(steps=horizon, exog=exog_future).predicted_mean.values
    fcst_dollars = fcst_per_asm * asm_fc
    return fcst_dollars


def main():
    df = pd.read_parquet(DATA_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df, feature_cols = build_features(df)

    horizon = 12
    start_idx = int(len(df) * 0.7)  # train on 70% then roll

    records = []
    for split in range(start_idx, len(df) - horizon):
        train = df.iloc[:split].copy()
        test = df.iloc[split : split + horizon].copy()
        test_end = test["date"].iloc[-1]

        # Skip if train period crosses known regime breaks (optional later analysis)
        regime = "full"
        if test_end >= pd.to_datetime("2004-01-01") and test_end <= pd.to_datetime("2014-12-31"):
            regime = "2004-2014"
        elif test_end >= pd.to_datetime("2015-01-01") and test_end <= pd.to_datetime("2019-12-31"):
            regime = "2015-2019"
        elif test_end >= pd.to_datetime("2020-01-01"):
            regime = "2020+"

        ols_preds = forecast_ols(train, feature_cols, horizon)
        arimax_preds = forecast_arimax(train, horizon)
        for i in range(horizon):
            records.append(
                {
                    "date": test["date"].iloc[i],
                    "Actual": test["Operating_PL"].iloc[i],
                    "OLS_Pred": ols_preds[i],
                    "ARIMAX_Pred": arimax_preds[i],
                    "regime": regime,
                }
            )

    out = pd.DataFrame(records)
    OUT_PATH.parent.mkdir(exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote backtest results -> {OUT_PATH}")


if __name__ == "__main__":
    main()
