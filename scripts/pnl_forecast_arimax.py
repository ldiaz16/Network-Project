"""
ARIMAX-based operating P/L forecaster using quarterly revenue and fuel (and optional ops).

Inputs:
- data/us_passenger_quarterly.parquet if present (preferred)
- Otherwise falls back to quarterly Operating_Rev/Operating_PL from US Passenger Data (All airlines)

Exogenous: Operating_Revenue (always), jet_fuel_usgc (if available), and optional Passengers/ASM/RPM/Flights/LoadFactor.

Outputs:
- reports/pnl_forecast_arimax.csv
- reports/pnl_forecast_arimax.png
"""

import pathlib
import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from statsmodels.tsa.statespace.sarimax import SARIMAX

DATA_DIR = pathlib.Path("US Passenger Data (All airlines)")
REV_FILE = DATA_DIR / "Operating_Rev_11_30_2025 12_55_09 AM.xls"
PL_FILE = DATA_DIR / "Operating_PL_11_30_2025 12_55_04 AM.xls"
EXOG_PATH = pathlib.Path("data") / "us_passenger_monthly_with_yield.parquet"
OUT_CSV = pathlib.Path("reports") / "pnl_forecast_arimax.csv"
OUT_PNG = pathlib.Path("reports") / "pnl_forecast_arimax.png"


def load_quarterly(path: pathlib.Path, value_name: str) -> pd.DataFrame:
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


def get_dataset() -> pd.DataFrame:
    if EXOG_PATH.exists():
        df = pd.read_parquet(EXOG_PATH)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").set_index("date")
    else:
        rev = load_quarterly(REV_FILE, "Operating_Revenue")
        pl = load_quarterly(PL_FILE, "Operating_PL")
        df = rev.merge(pl, on="date", how="inner")
        df = df.sort_values("date").set_index("date")
    df = df.interpolate(limit_direction="both")
    # Normalize target by ASM to stabilize scale
    if "ASM" in df.columns:
        df["pl_per_asm"] = df["Operating_PL"] / df["ASM"]
    else:
        df["pl_per_asm"] = df["Operating_PL"]
    return df


def build_exog(df: pd.DataFrame, horizon: int):
    exog_cols = [c for c in df.columns if c != "Operating_PL"]
    exog_hist = df[exog_cols]
    # Drop columns that are all NaN and fill gaps
    exog_hist = exog_hist.dropna(axis=1, how="all").ffill().bfill()

    def seasonal_growth_forecast(
        series: pd.Series, horizon: int, season: int = 12, lookback: int = 24, cap: float = 0.10
    ):
        """
        Seasonal naive with multiplicative growth using recent median monthly pct change.
        Keeps seasonality from the last full season to avoid over-smoothing spikes.
        """
        values = series.to_numpy()
        if len(values) == 0:
            return np.zeros(horizon)
        if len(values) >= season:
            season_base = values[-season:]
        else:
            season_base = np.resize(values, season)
        # Recent growth rate (median of last lookback pct changes, bounded)
        pct_changes = pd.Series(values).pct_change().dropna()
        if len(pct_changes) > 0:
            recent = pct_changes.tail(lookback)
            g = recent.median()
            g = float(np.clip(g, -cap, cap))  # cap extreme growth
        else:
            g = 0.0
        forecasts = []
        for i in range(horizon):
            season_val = season_base[i % season]
            grow_factor = (1 + g) ** ((i + 1) / float(season))
            forecasts.append(season_val * grow_factor)
        return np.array(forecasts)

    # Operating revenue seasonal + trend (already per ASM compatible)
    rev_forecast = (
        seasonal_growth_forecast(exog_hist["Operating_Revenue"], horizon)
        if "Operating_Revenue" in exog_hist
        else None
    )

    # Fuel: seasonal + bounded growth if enough history; otherwise AR(1)
    if "jet_fuel_usgc" in exog_hist:
        fuel_series = exog_hist["jet_fuel_usgc"]
        if len(fuel_series) >= 24:
            fuel_forecast = seasonal_growth_forecast(fuel_series, horizon, lookback=12)
        else:
            vals = fuel_series.to_numpy()
            phi = np.corrcoef(vals[:-1], vals[1:])[0, 1] if len(vals) >= 2 else 0.0
            fuel_last = vals[-1]
            fuel_forecast = []
            for _ in range(horizon):
                fuel_next = phi * fuel_last + (1 - phi) * fuel_last
                fuel_forecast.append(fuel_next)
                fuel_last = fuel_next
            fuel_forecast = np.array(fuel_forecast)
    else:
        fuel_forecast = None

    # ASM forecast
    asm_forecast = (
        seasonal_growth_forecast(exog_hist["ASM"], horizon, lookback=24) if "ASM" in exog_hist else np.ones(horizon)
    )

    # Yield forecasts (per RPM/pax/ASM) if present
    yield_cols = ["yield_rev_per_rpm", "yield_rev_per_pax", "rev_per_asm"]
    yield_forecasts = {}
    for ycol in yield_cols:
        if ycol in exog_hist:
            yield_forecasts[ycol] = seasonal_growth_forecast(exog_hist[ycol], horizon, lookback=24, cap=0.10)

    # Shock dummies to let model react to large moves; future shocks set to 0
    shocks = pd.Series(0, index=df.index, dtype=float)
    diffs = df["Operating_PL"].diff()
    threshold = diffs.abs().quantile(0.95)
    shocks.loc[diffs.abs() >= threshold] = 1.0
    exog_hist = exog_hist.assign(shock=shocks)

    last = exog_hist.iloc[-1]
    future_rows = []
    asm_future = []
    for i in range(horizon):
        row = last.copy()
        row["shock"] = 0.0
        if rev_forecast is not None:
            row["Operating_Revenue"] = rev_forecast[i]
        if fuel_forecast is not None:
            row["jet_fuel_usgc"] = fuel_forecast[i]
        if "ASM" in exog_hist:
            row["ASM"] = asm_forecast[i]
        for ycol, yfc in yield_forecasts.items():
            row[ycol] = yfc[i]
        asm_future.append(row.get("ASM", 1.0))
        future_rows.append(row)
    exog_future = pd.DataFrame(future_rows, columns=exog_hist.columns)
    return exog_hist, exog_future, np.array(exog_hist["ASM"]) if "ASM" in exog_hist else np.ones(len(exog_hist)), np.array(asm_future)


def fit_forecast(df: pd.DataFrame, horizon: int = 4):
    y = df["pl_per_asm"]
    exog_hist, exog_future, asm_hist, asm_future = build_exog(df, horizon)
    # Simpler ARIMAX spec to reduce phase lag; include constant
    model = SARIMAX(
        y,
        exog=exog_hist,
        order=(1, 0, 1),
        seasonal_order=(1, 0, 1, 12),
        enforce_stationarity=False,
        enforce_invertibility=False,
        trend="c",
    )
    res = model.fit(method="powell", disp=False)
    forecast_res = res.get_forecast(steps=horizon, exog=exog_future)
    fcst_per_asm = forecast_res.predicted_mean
    conf_per_asm = forecast_res.conf_int()
    fitted_per_asm = res.fittedvalues
    # Convert back to dollar P/L using ASM hist/future
    fcst = fcst_per_asm * asm_future
    conf = conf_per_asm.multiply(asm_future, axis=0)
    fitted = fitted_per_asm * asm_hist
    return res, fcst, conf, fitted


def main():
    df = get_dataset()
    horizon = 12  # forecast 12 months ahead
    res, fcst, conf, fitted = fit_forecast(df, horizon=horizon)

    hist = df.reset_index()[["date", "Operating_PL"]].copy()
    hist["Type"] = "history"
    hist["Predicted"] = fitted.values
    hist["Lower"] = np.nan
    hist["Upper"] = np.nan

    future_dates = pd.date_range(hist["date"].iloc[-1] + pd.offsets.MonthEnd(), periods=horizon, freq="M")
    fcst_df = pd.DataFrame(
        {
            "date": future_dates,
            "Operating_PL": np.nan,
            "Predicted": fcst.values,
            "Lower": conf.iloc[:, 0].values,
            "Upper": conf.iloc[:, 1].values,
            "Type": "forecast",
        }
    )

    out = pd.concat([hist, fcst_df], ignore_index=True)
    OUT_CSV.parent.mkdir(exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    print(f"Saved ARIMAX forecast data -> {OUT_CSV}")

    # Plot
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(hist["date"], hist["Operating_PL"], label="Actual P/L", color="black", linewidth=1.6)
    ax.plot(hist["date"], hist["Predicted"], label="Fitted (ARIMAX)", color="blue", linestyle="--", linewidth=1.4)
    ax.plot(fcst_df["date"], fcst_df["Predicted"], label="Forecast (ARIMAX)", color="green", linestyle="--")
    ax.fill_between(fcst_df["date"], fcst_df["Lower"], fcst_df["Upper"], color="green", alpha=0.2, label="95% CI")

    # Trend lines (linear) for actuals and fitted
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

    ax.set_title("Operating P/L Forecast (ARIMAX)")
    ax.set_ylabel("P/L (TOTAL)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    fig.savefig(OUT_PNG, dpi=150, bbox_inches="tight")
    print(f"Saved ARIMAX plot -> {OUT_PNG}")


if __name__ == "__main__":
    main()
