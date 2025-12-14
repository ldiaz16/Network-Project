"""
Simple VAR backtest for per-ASM metrics.

Series used:
  - pl_per_asm: Operating_PL / ASM
  - rev_per_asm: Operating_Revenue / ASM
  - ASM
  - LoadFactor (if available)
  - jet_fuel_usgc (if available)

Outputs:
  - reports/backtest_var.csv (rolling 12-month forecast backtest on 70/30 split)
"""

import pathlib
import numpy as np
import pandas as pd
from statsmodels.tsa.api import VAR

DATA_PATH = pathlib.Path("data/us_passenger_monthly.parquet")
OUT_PATH = pathlib.Path("reports/backtest_var.csv")


def load_series():
    df = pd.read_parquet(DATA_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    if "ASM" in df.columns:
        df["pl_per_asm"] = df["Operating_PL"] / df["ASM"]
        df["rev_per_asm"] = df["Operating_Revenue"] / df["ASM"]
    else:
        df["pl_per_asm"] = df["Operating_PL"]
        df["rev_per_asm"] = df["Operating_Revenue"]
    use_cols = ["pl_per_asm", "rev_per_asm", "ASM"]
    for col in ["LoadFactor", "jet_fuel_usgc"]:
        if col in df.columns:
            use_cols.append(col)
    df = df[["date"] + use_cols].set_index("date")
    df = df.apply(pd.to_numeric, errors="coerce").interpolate(limit_direction="both")
    return df


def backtest_var(df: pd.DataFrame, horizon: int = 12):
    start_idx = int(len(df) * 0.7)
    records = []
    for split in range(start_idx, len(df) - horizon):
        train = df.iloc[:split]
        test = df.iloc[split : split + horizon]
        model = VAR(train)
        res = model.fit(maxlags=6, ic="aic")
        fcst = res.forecast(train.values, steps=horizon)
        fcst_idx = test.index
        fcst_df = pd.DataFrame(fcst, columns=train.columns, index=fcst_idx)
        for i in range(horizon):
            records.append(
                {
                    "date": fcst_idx[i],
                    "Actual_pl_per_asm": test["pl_per_asm"].iloc[i],
                    "Forecast_pl_per_asm": fcst_df["pl_per_asm"].iloc[i],
                }
            )
    out = pd.DataFrame(records)
    return out


def main():
    df = load_series()
    out = backtest_var(df, horizon=12)
    OUT_PATH.parent.mkdir(exist_ok=True)
    out.to_csv(OUT_PATH, index=False)
    print(f"Wrote VAR backtest -> {OUT_PATH}")
    # Simple MAE/SMAPE on per-ASM for quick readout
    err = (out["Forecast_pl_per_asm"] - out["Actual_pl_per_asm"]).abs()
    mae = err.mean()
    denom = (out["Forecast_pl_per_asm"].abs() + out["Actual_pl_per_asm"].abs()).replace(0, pd.NA)
    smape = (err / denom * 2).dropna().mean()
    print(f"MAE (per ASM): {mae:,.4f}")
    print(f"SMAPE (per ASM): {smape:.3f}")


if __name__ == "__main__":
    main()
