"""
Aggregate monthly US Passenger Data (all airlines) and fuel into a monthly feature set.

Revenue/PL quarterly values are allocated to months weighted by monthly ASM (when available).

Outputs:
  data/us_passenger_monthly.parquet with columns:
    date (month end)
    Passengers, ASM, RPM, Flights, LoadFactor, Operating_Revenue, Operating_PL, jet_fuel_usgc
"""

import pathlib
import pandas as pd

DATA_DIR = pathlib.Path("US Passenger Data (All airlines)")
OUT_PATH = pathlib.Path("data") / "us_passenger_monthly.parquet"
FUEL_PATH = pathlib.Path("data") / "fuel_prices.parquet"

MONTHLY_FILES = {
    "Passengers": "Passengers_11_30_2025 12_54_07 AM.xls",
    "ASM": "ASM_11_30_2025 12_54_45 AM.xls",
    "RPM": "RPM_11_30_2025 12_54_40 AM.xls",
    "Flights": "Flights_11_30_2025 12_54_34 AM.xls",
    "LoadFactor": "LoadFactor_11_30_2025 12_54_52 AM.xls",
}

QUARTERLY_FILES = {
    "Operating_Revenue": "Operating_Rev_11_30_2025 12_55_09 AM.xls",
    "Operating_PL": "Operating_PL_11_30_2025 12_55_04 AM.xls",
}


def load_monthly(name: str, filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    df = pd.read_html(path)[0]
    df = df[df["Month"].apply(lambda x: str(x).isdigit())].copy()
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["Month"] = pd.to_numeric(df["Month"], errors="coerce")
    df = df.dropna(subset=["Year", "Month"])
    df["date"] = pd.to_datetime({"year": df["Year"].astype(int), "month": df["Month"].astype(int), "day": 1})
    df[name] = pd.to_numeric(df["TOTAL"], errors="coerce")
    return df[["date", name]]


def load_quarterly(name: str, filename: str) -> pd.DataFrame:
    path = DATA_DIR / filename
    df = pd.read_html(path)[0]
    df = df[df["Quarter"].apply(lambda x: str(x).isdigit())].copy()
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
    df["Quarter"] = pd.to_numeric(df["Quarter"], errors="coerce")
    df = df.dropna(subset=["Year", "Quarter"])
    df["date"] = pd.PeriodIndex(year=df["Year"].astype(int), quarter=df["Quarter"].astype(int)).to_timestamp(how="end")
    df[name] = pd.to_numeric(df["TOTAL"], errors="coerce")
    return df[["date", name]]


def allocate_quarterly_to_months(qdf: pd.DataFrame, monthly_asm: pd.DataFrame) -> pd.DataFrame:
    # For each quarter, distribute value across its three months proportional to ASM (fallback equal split)
    qdf = qdf.copy()
    monthly_asm = monthly_asm.copy()
    monthly_asm["quarter"] = monthly_asm["date"].dt.to_period("Q")
    allocations = []
    for _, row in qdf.iterrows():
        q_end = row["date"]
        q_period = q_end.to_period("Q")
        months = pd.period_range(end=q_end, periods=3, freq="M").to_timestamp()
        asm_slice = monthly_asm[monthly_asm["quarter"] == q_period]
        if asm_slice.empty or asm_slice["ASM"].isna().all():
            weights = pd.Series([1 / 3] * 3, index=months)
        else:
            asm_vals = asm_slice.set_index("date").reindex(months)["ASM"]
            asm_vals = asm_vals.fillna(asm_vals.mean())
            weights = asm_vals / asm_vals.sum()
        for m in months:
            allocations.append({"date": m, row.index[1]: row.iloc[1] * weights.loc[m]})
    alloc_df = pd.DataFrame(allocations).groupby("date").sum().reset_index()
    return alloc_df


def main():
    monthly = [load_monthly(name, fname) for name, fname in MONTHLY_FILES.items()]
    df_month = monthly[0]
    for extra in monthly[1:]:
        df_month = df_month.merge(extra, on="date", how="outer")
    df_month = df_month.sort_values("date")

    # Allocate quarterly revenue/PL to months using ASM weights
    asm_for_weights = df_month[["date", "ASM"]].copy()
    for name, fname in QUARTERLY_FILES.items():
        qdf = load_quarterly(name, fname)
        alloc = allocate_quarterly_to_months(qdf, asm_for_weights)
        df_month = df_month.merge(alloc, on="date", how="left")

    # Merge fuel
    if FUEL_PATH.exists():
        fuel_df = pd.read_parquet(FUEL_PATH)
        fuel_df["date"] = pd.to_datetime(fuel_df["date"])
        fuel_df["date"] = fuel_df["date"].dt.to_period("M").dt.to_timestamp()
        df_month = df_month.merge(fuel_df, on="date", how="left")

    # Fill gaps
    df_month = df_month.sort_values("date")
    df_month = df_month.ffill().bfill()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_month.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {len(df_month)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
