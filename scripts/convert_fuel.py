"""
Convert fuel price timeseries to a clean Parquet for forecasting.

Sources:
- Weekly U.S. Gulf Coast Kerosene-Type Jet Fuel Spot Price (FOB) in
  "Fuel Data/EER_EPJK_PF4_RGC_DPGw.xls" sheet "Data 1".
- Monthly BTS fuel cost averages in "Fuel Data/BTS Fuel Costs.xlsx" (if present).

Outputs:
  data/fuel_prices.parquet with columns:
    date (month end), jet_fuel_usgc (dollars per gallon)
"""

import pathlib
import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "Fuel Data"
OUT_PATH = BASE_DIR / "data" / "fuel_prices.parquet"


def load_weekly_usgc(path: pathlib.Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path, engine="xlrd")
    df = xls.parse("Data 1")
    # The first column is date, second is price
    df = df.dropna()
    df.columns = ["date", "price"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["date", "price"])
    # Resample to month end average
    monthly = df.set_index("date").resample("M").mean().reset_index()
    monthly = monthly.rename(columns={"price": "jet_fuel_usgc"})
    return monthly


def load_bts_monthly(path: pathlib.Path) -> pd.DataFrame:
    # Assume first sheet, columns Date and Price
    df = pd.read_excel(path)
    # Try to find date and price columns heuristically
    cols = {c.lower(): c for c in df.columns}
    date_col = None
    for key in ("date", "period", "month"):
        if key in cols:
            date_col = cols[key]
            break
    price_col = None
    for key in ("price", "cost", "avg", "fuel"):
        for col in df.columns:
            if key in col.lower():
                price_col = col
                break
        if price_col:
            break
    if not date_col or not price_col:
        return pd.DataFrame(columns=["date", "jet_fuel_usgc"])
    df = df[[date_col, price_col]].copy()
    df.columns = ["date", "jet_fuel_usgc"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["jet_fuel_usgc"] = pd.to_numeric(df["jet_fuel_usgc"], errors="coerce")
    df = df.dropna(subset=["date", "jet_fuel_usgc"])
    return df


def main():
    frames = []
    weekly_path = DATA_DIR / "EER_EPJK_PF4_RGC_DPGw.xls"
    if weekly_path.exists():
        frames.append(load_weekly_usgc(weekly_path))
    monthly_path = DATA_DIR / "BTS Fuel Costs.xlsx"
    if monthly_path.exists():
        frames.append(load_bts_monthly(monthly_path))

    if not frames:
        raise SystemExit("No fuel data found.")

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("date")
    df = df.drop_duplicates(subset=["date"], keep="last")
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {len(df)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
