"""
VAR-based operating P/L forecaster using quarterly revenue, P/L, and exogenous ops/fuel.

Inputs:
- data/us_passenger_quarterly.parquet (Passengers, ASM, RPM, Flights, LoadFactor, jet_fuel_usgc, Operating_Revenue, Operating_PL, date)
- US Passenger Data (All airlines)/Operating_Rev_*.xls and Operating_PL_*.xls as fallback if parquet missing.

Outputs:
- reports/pnl_forecast_var.csv
- reports/pnl_forecast_var.png
"""

import pathlib
import numpy as np
import pandas as pd
from statsmodels.tsa.api import VAR
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA_DIR = pathlib.Path("US Passenger Data (All airlines)")
REV_FILE = DATA_DIR / "Operating_Rev_11_30_2025 12_55_09 AM.xls"
PL_FILE = DATA_DIR / "Operating_PL_11_30_2025 12_55_04 AM.xls"
EXOG_PATH = pathlib.Path("data") / "us_passenger_quarterly.parquet"
OUT_CSV = pathlib.Path("reports") / "pnl_forecast_var.csv"
OUT_PNG = pathlib.Path("reports") / "pnl_forecast_var.png"


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


def ensure_quarterly_dataset() -> pd.DataFrame:
    # Base revenue/PL from XLS
    if not (REV_FILE.exists() and PL_FILE.exists()):
        raise SystemExit("No quarterly revenue/PL files.")
    rev = load_quarterly(REV_FILE, "Operating_Revenue")
    pl = load_quarterly(PL_FILE, "Operating_PL")
    base = rev.merge(pl, on="date", how="inner")
    # Merge exogenous if available
    if EXOG_PATH.exists():
        exog = pd.read_parquet(EXOG_PATH)
        exog["date"] = pd.to_datetime(exog["date"])
        base = base.merge(exog, on="date", how="left", suffixes=("_x", "_y"))
        # Reconcile potential duplicates
        if "Operating_Revenue_x" in base.columns:
            base["Operating_Revenue"] = base["Operating_Revenue_x"].fillna(base.get("Operating_Revenue_y"))
        if "Operating_PL_x" in base.columns:
            base["Operating_PL"] = base["Operating_PL_x"].fillna(base.get("Operating_PL_y"))
        drop_dups = [c for c in base.columns if c.endswith("_x") or c.endswith("_y")]
        base = base.drop(columns=drop_dups)
    base = base.sort_values("date")
    return base


def fit_var(df: pd.DataFrame, horizon: int = 4) -> pd.DataFrame:
    df = df.set_index("date")
    # Use core series only to maintain density
    vars_order = ["Operating_Revenue", "Operating_PL", "jet_fuel_usgc"]
    avail = [v for v in vars_order if v in df.columns]
    if "Operating_PL" not in avail or "Operating_Revenue" not in avail:
        raise SystemExit("Operating_PL and Operating_Revenue are required.")
    y = df[avail].copy()
    y = y.interpolate(limit_direction="both")
    y = y.dropna()
    if len(y) < 8:
        raise SystemExit("Not enough data points for VAR.")
    model = VAR(y)
    res = model.fit(maxlags=1, ic=None)
    forecast = res.forecast_interval(y.values[-res.k_ar :], steps=horizon, alpha=0.05)
    preds, lower, upper = forecast
    future_idx = pd.date_range(y.index[-1] + pd.offsets.QuarterEnd(), periods=horizon, freq="Q")

    fcst = pd.DataFrame(preds, columns=avail, index=future_idx)
    fcst_lower = pd.DataFrame(lower, columns=avail, index=future_idx)
    fcst_upper = pd.DataFrame(upper, columns=avail, index=future_idx)

    hist = y.reset_index().rename(columns={"index": "date"})
    hist["Type"] = "history"
    fcst = fcst.reset_index().rename(columns={"index": "date"})
    fcst_lower = fcst_lower.reset_index().rename(columns={"index": "date"})
    fcst_upper = fcst_upper.reset_index().rename(columns={"index": "date"})
    fcst["Type"] = "forecast"

    out = []
    for df_fcst, band in ((fcst, None), (fcst_lower, "Lower"), (fcst_upper, "Upper")):
        tmp = df_fcst[["date", "Operating_PL"]].copy()
        if band:
            tmp["Band"] = band
        out.append(tmp)
    return hist, fcst, fcst_lower, fcst_upper


def plot_var(hist: pd.DataFrame, fcst: pd.DataFrame, fcst_lower: pd.DataFrame, fcst_upper: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(hist["date"], hist["Operating_PL"], label="Actual P/L", color="black")
    ax.plot(fcst["date"], fcst["Operating_PL"], label="Forecast (VAR)", color="orange", linestyle="--")
    ax.fill_between(
        fcst["date"],
        fcst_lower["Operating_PL"],
        fcst_upper["Operating_PL"],
        color="orange",
        alpha=0.2,
        label="95% CI",
    )
    ax.set_title("Operating P/L Forecast (VAR)")
    ax.set_ylabel("P/L (TOTAL)")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.autofmt_xdate()
    OUT_PNG.parent.mkdir(exist_ok=True)
    fig.savefig(OUT_PNG, dpi=150, bbox_inches="tight")
    print(f"Saved VAR plot -> {OUT_PNG}")


def main():
    df = ensure_quarterly_dataset()
    hist, fcst, fcst_lower, fcst_upper = fit_var(df, horizon=4)
    # Save combined CSV
    hist_out = hist[["date", "Operating_PL"]].copy()
    hist_out["Type"] = "history"
    fcst_out = fcst[["date", "Operating_PL"]].copy()
    fcst_out["Type"] = "forecast"
    fcst_out["Lower"] = fcst_lower["Operating_PL"]
    fcst_out["Upper"] = fcst_upper["Operating_PL"]
    combined = pd.concat([hist_out, fcst_out], ignore_index=True)
    OUT_CSV.parent.mkdir(exist_ok=True)
    combined.to_csv(OUT_CSV, index=False)
    print(f"Saved VAR forecast data -> {OUT_CSV}")
    plot_var(hist, fcst, fcst_lower, fcst_upper)


if __name__ == "__main__":
    main()
