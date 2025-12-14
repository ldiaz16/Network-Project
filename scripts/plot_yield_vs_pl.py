"""
Plot yield vs Operating P/L to visualize correlation.
Reads: data/us_passenger_monthly_with_yield.parquet
Outputs: reports/yield_vs_pl.png
"""

import pathlib
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATA_PATH = pathlib.Path("data/us_passenger_monthly_with_yield.parquet")
OUT_PNG = pathlib.Path("reports") / "yield_vs_pl.png"


def main():
    df = pd.read_parquet(DATA_PATH)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    # Basic scatter of yield vs P/L, colored by year
    df["year"] = df["date"].dt.year
    plt.style.use("seaborn-v0_8-whitegrid")
    fig, ax = plt.subplots(figsize=(8, 5))
    sc = ax.scatter(
        df["yield_rev_per_rpm"],
        df["Operating_PL"],
        c=df["year"],
        cmap="viridis",
        s=18,
        alpha=0.7,
        edgecolor="none",
    )
    cbar = fig.colorbar(sc, ax=ax, label="Year")
    ax.set_xlabel("Yield (Revenue per RPM)")
    ax.set_ylabel("Operating P/L")
    ax.set_title("Yield vs Operating P/L (color = year)")
    # Add trend line (OLS)
    x = df["yield_rev_per_rpm"]
    y = df["Operating_PL"]
    mask = x.notna() & y.notna()
    if mask.sum() > 2:
        import numpy as np

        coeffs = pd.Series(x[mask]).to_numpy()
        yy = pd.Series(y[mask]).to_numpy()
        slope, intercept = np.polyfit(coeffs, yy, deg=1)
        x_line = np.linspace(x.min(), x.max(), 100)
        ax.plot(x_line, slope * x_line + intercept, color="red", linestyle="--", label="Trend")
        ax.legend()
    OUT_PNG.parent.mkdir(exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUT_PNG, dpi=150)
    print(f"Saved plot -> {OUT_PNG}")


if __name__ == "__main__":
    main()
