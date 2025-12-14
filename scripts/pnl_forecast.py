"""
Simple operating P/L forecaster using the quarterly Operating_Rev and Operating_PL tables
from `US Passenger Data (All airlines)`.

What it does:
- Parse quarterly revenue and P/L from the HTML-exported XLS files.
- Build a design matrix with trend, seasonality (quarter), lagged P/L and revenue.
- Fit a linear regression, estimate residual std for confidence intervals.
- Forecast the next 4 quarters using a simple revenue forecast (trend + seasonality) fed into the P/L model.
- Save a plot and a CSV of actuals/predictions with confidence intervals.

Outputs:
  reports/pnl_forecast.png
  reports/pnl_forecast.csv
"""

import pathlib
from dataclasses import dataclass
from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
import matplotlib

# Use headless backend
matplotlib.use("Agg")
import matplotlib.pyplot as plt


DATA_DIR = pathlib.Path("US Passenger Data (All airlines)")
REV_FILE = DATA_DIR / "Operating_Rev_11_30_2025 12_55_09 AM.xls"
PL_FILE = DATA_DIR / "Operating_PL_11_30_2025 12_55_04 AM.xls"
EXOG_MONTHLY_PATH = pathlib.Path("data") / "us_passenger_monthly.parquet"
OUTPUT_DIR = pathlib.Path("reports")
OUTPUT_PNG = OUTPUT_DIR / "pnl_forecast.png"
OUTPUT_CSV = OUTPUT_DIR / "pnl_forecast.csv"


def load_quarterly(path: pathlib.Path, value_name: str) -> pd.DataFrame:
    """Load the HTML-exported XLS and return Year, Quarter, value, date."""
    df = pd.read_html(path)[0]
    df = df[df["Quarter"].apply(lambda x: str(x).isdigit())].copy()
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["Quarter"] = pd.to_numeric(df["Quarter"], errors="coerce")
    df = df.dropna(subset=["Year", "Quarter"])
    df["date"] = pd.PeriodIndex(year=df["Year"].astype(int), quarter=df["Quarter"].astype(int)).to_timestamp(
        how="end"
    )
    df = df[["date", "TOTAL"]].rename(columns={"TOTAL": value_name})
    return df.sort_values("date")


def make_design(df: pd.DataFrame, target_col: str) -> Tuple[pd.DataFrame, pd.Series, list, pd.DataFrame]:
    """Create features: trend, month seasonality (sin/cos), lagged P/L and revenue, exogenous ops."""
    df = df.copy()
    df["trend"] = np.arange(len(df))
    df["month"] = df["date"].dt.month
    df["m_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["m_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    # Lags
    df["p_lag1"] = df[target_col].shift(1)
    df["p_lag2"] = df[target_col].shift(2)
    df["rev_lag1"] = df["Operating_Revenue"].shift(1)
    df["rev_lag2"] = df["Operating_Revenue"].shift(2)
    feature_cols = ["trend", "m_sin", "m_cos", "p_lag1", "p_lag2", "rev_lag1", "rev_lag2", "Operating_Revenue"]
    # Add exogenous ops if present
    for col in ("Passengers", "ASM", "RPM", "Flights", "LoadFactor"):
        if col in df.columns:
            feature_cols.append(col)
    if "jet_fuel_usgc" in df.columns:
        feature_cols.append("jet_fuel_usgc")
    # Coerce to numeric and fill gaps
    df[feature_cols] = df[feature_cols].apply(pd.to_numeric, errors="coerce")
    df[feature_cols] = df[feature_cols].ffill().bfill().fillna(0)
    # Only drop if target missing; keep exogenous filled
    df = df.dropna(subset=[target_col])
    X = df[feature_cols]
    y = df[target_col]
    return X, y, feature_cols, df


def forecast_revenue(df: pd.DataFrame, periods: int) -> pd.Series:
    """Forecast revenue using a simple trend + seasonality regression (monthly)."""
    rev_df = df[["date", "Operating_Revenue"]].copy()
    rev_df["trend"] = np.arange(len(rev_df))
    rev_df["m_sin"] = np.sin(2 * np.pi * rev_df["date"].dt.month / 12)
    rev_df["m_cos"] = np.cos(2 * np.pi * rev_df["date"].dt.month / 12)
    X = rev_df[["trend", "m_sin", "m_cos"]]
    y = rev_df["Operating_Revenue"]
    model = LinearRegression().fit(X, y)

    future_trend = np.arange(len(rev_df), len(rev_df) + periods)
    last_month = rev_df["date"].dt.month.iloc[-1]
    future_month = ((last_month + np.arange(1, periods + 1) - 1) % 12) + 1
    fut = pd.DataFrame(
        {
            "trend": future_trend,
            "m_sin": np.sin(2 * np.pi * future_month / 12),
            "m_cos": np.cos(2 * np.pi * future_month / 12),
        }
    )
    preds = model.predict(fut)
    return pd.Series(preds)


def build_forecast(df: pd.DataFrame, horizon: int = 4) -> pd.DataFrame:
    """Fit P/L model and forecast next `horizon` quarters."""
    if "Operating_PL" not in df.columns:
        raise SystemExit("Operating_PL missing after merge; cannot forecast.")
    X, y, feature_cols, df_clean = make_design(df, target_col="Operating_PL")
    model = LinearRegression().fit(X, y)
    y_pred = model.predict(X)
    resid_std = np.std(y - y_pred)

    # Historical with predictions
    hist = df.loc[X.index, ["date", "Operating_PL"]].copy()
    hist["Predicted"] = y_pred
    hist["Lower"] = hist["Predicted"] - 1.96 * resid_std
    hist["Upper"] = hist["Predicted"] + 1.96 * resid_std
    hist["Type"] = "history"

    # Forecast revenue and build future rows
    future_rev = forecast_revenue(df_clean, periods=horizon)
    last_row = df_clean.iloc[-1]
    future_dates = pd.date_range(start=last_row["date"] + pd.offsets.MonthEnd(), periods=horizon, freq="M")

    future = []
    # Prepare future lagged features using last known values
    p_hist = df_clean["Operating_PL"].tolist()
    rev_hist = df_clean["Operating_Revenue"].tolist()
    for i in range(horizon):
        p_lag1 = p_hist[-1] if i == 0 else future[-1]["Predicted"]
        p_lag2 = p_hist[-2] if i == 0 else (p_hist[-1] if i == 1 else future[-2]["Predicted"])
        rev_lag1 = rev_hist[-1] if i == 0 else future_rev.iloc[i - 1]
        rev_lag2 = rev_hist[-2] if i == 0 else (rev_hist[-1] if i == 1 else future_rev.iloc[i - 2])
        quarter = future_dates[i].quarter
        feats_dict = {
            "trend": [len(df_clean) + i],
            "q_sin": [np.sin(2 * np.pi * quarter / 4)],
            "q_cos": [np.cos(2 * np.pi * quarter / 4)],
            "p_lag1": [p_lag1],
            "p_lag2": [p_lag2],
            "rev_lag1": [rev_lag1],
            "rev_lag2": [rev_lag2],
            "Operating_Revenue": [future_rev.iloc[i]],
        }
        # add any extra exogenous columns with last observed values
        for col in feature_cols:
            if col not in feats_dict:
                feats_dict[col] = [last_row.get(col, 0)]
        feats = pd.DataFrame(feats_dict)
        feats = feats[feature_cols]
        pred = model.predict(feats)[0]
        future.append(
            {
                "date": future_dates[i],
                "Operating_PL": np.nan,
                "Predicted": pred,
                "Lower": pred - 1.96 * resid_std,
                "Upper": pred + 1.96 * resid_std,
                "Type": "forecast",
            }
        )
    future_df = pd.DataFrame(future)

    return pd.concat([hist, future_df], ignore_index=True)


def plot_forecast(df: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 5))
    hist = df[df["Type"] == "history"]
    fcst = df[df["Type"] == "forecast"]
    ax.plot(hist["date"], hist["Operating_PL"], label="Actual P/L", color="black")
    ax.plot(hist["date"], hist["Predicted"], label="Fitted", color="blue")
    ax.plot(fcst["date"], fcst["Predicted"], label="Forecast", color="orange", linestyle="--")
    ax.fill_between(fcst["date"], fcst["Lower"], fcst["Upper"], color="orange", alpha=0.2, label="95% CI")
    ax.set_title("Operating P/L (Actual/Fitted + 4Q Forecast)")
    ax.set_ylabel("P/L (TOTAL)")
    ax.grid(True, alpha=0.3)

    # Trend lines for actual and fitted
    def add_trend(x_dates, y_vals, label, color, linestyle=":"):
        x_num = np.arange(len(x_dates))
        mask = ~pd.isna(y_vals)
        if mask.sum() < 2:
            return
        coeffs = np.polyfit(x_num[mask], y_vals[mask], deg=1)
        trend = coeffs[0] * x_num + coeffs[1]
        ax.plot(x_dates, trend, color=color, linestyle=linestyle, label=label)

    add_trend(hist["date"], hist["Operating_PL"], "Actual trend", "black", linestyle="-.")
    add_trend(hist["date"], hist["Predicted"], "Fitted trend", "blue", linestyle=":")

    ax.legend()
    fig.autofmt_xdate()
    OUTPUT_DIR.mkdir(exist_ok=True)
    fig.savefig(OUTPUT_PNG, dpi=150, bbox_inches="tight")
    print(f"Saved plot -> {OUTPUT_PNG}")


def main():
    if EXOG_MONTHLY_PATH.exists():
        merged = pd.read_parquet(EXOG_MONTHLY_PATH)
        merged["date"] = pd.to_datetime(merged["date"])
        merged = merged.sort_values("date")
    else:
        if not REV_FILE.exists() or not PL_FILE.exists():
            raise SystemExit("Operating Rev/PL files not found in 'US Passenger Data (All airlines)'.")
        rev = load_quarterly(REV_FILE, "Operating_Revenue")
        pl = load_quarterly(PL_FILE, "Operating_PL")
        merged = rev.merge(pl, on="date", how="inner").sort_values("date")

    if merged.empty:
        raise SystemExit("No overlapping observations in revenue and P/L files.")

    # Use 12-month horizon for monthly data
    result = build_forecast(merged, horizon=12)
    OUTPUT_DIR.mkdir(exist_ok=True)
    result.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved forecast data -> {OUTPUT_CSV}")
    plot_forecast(result)


if __name__ == "__main__":
    main()
