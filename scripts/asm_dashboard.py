import argparse
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a Markdown dashboard that summarizes ASM data-quality history."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("reports/asm_summary_history.csv"),
        help="CSV produced by scripts/report.py or scripts/showcase.py.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("reports/asm_dashboard.md"),
        help="Destination Markdown file.",
    )
    parser.add_argument(
        "--airline",
        action="append",
        help="Limit the dashboard to one or more airlines.",
    )
    parser.add_argument(
        "--trend-rows",
        type=int,
        default=5,
        help="Number of historical rows to include in the trend tables.",
    )
    return parser.parse_args()


def _sanitize_cell(value):
    text = str(value)
    text = text.replace("\\", "\\\\")
    text = text.replace("|", "\\|")
    text = text.replace("\n", " ")
    return text


def df_to_markdown(df, columns=None, max_rows=None):
    if df is None or df.empty:
        return "_No data available._"
    subset = df[columns] if columns else df
    limited = subset if max_rows is None else subset.head(max_rows)
    limited = limited.fillna("—")
    headers = " | ".join(_sanitize_cell(col) for col in limited.columns)
    separator = " | ".join("---" for _ in limited.columns)
    rows = [
        " | ".join(_sanitize_cell(value) for value in row)
        for row in limited.to_numpy()
    ]
    table_lines = [f"| {headers} |", f"| {separator} |"]
    table_lines.extend(f"| {row} |" for row in rows)
    return "\n".join(table_lines)


def load_history(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    required = {
        "Airline",
        "Seat Source",
        "ASM Share Value",
        "ASM Share",
        "Total Seats",
        "Total ASM",
        "Generated At",
    }
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"History file is missing required columns: {', '.join(sorted(missing))}")
    df["Generated At"] = pd.to_datetime(df["Generated At"], utc=True, errors="coerce")
    df["ASM Share Value"] = pd.to_numeric(df["ASM Share Value"], errors="coerce").fillna(0.0)
    df["Total ASM"] = pd.to_numeric(df["Total ASM"], errors="coerce").fillna(0.0)
    df["Total Seats"] = pd.to_numeric(df["Total Seats"], errors="coerce").fillna(0.0)
    return df


def latest_snapshot(history: pd.DataFrame, airline: str):
    subset = history[history["Airline"] == airline]
    if subset.empty:
        return pd.DataFrame(), None
    latest_ts = subset["Generated At"].max()
    latest = subset[subset["Generated At"] == latest_ts].copy()
    latest = latest.sort_values("Total ASM", ascending=False).reset_index(drop=True)
    latest["ASM Share"] = latest["ASM Share"].astype(str)
    latest["Total Seats"] = latest["Total Seats"].astype(int)
    latest["Total ASM"] = latest["Total ASM"].astype(int)
    return latest, latest_ts


def build_trend(history: pd.DataFrame, airline: str, rows: int) -> pd.DataFrame:
    subset = history[history["Airline"] == airline]
    if subset.empty:
        return pd.DataFrame()
    pivot = (
        subset.pivot_table(
            index="Generated At",
            columns="Seat Source",
            values="ASM Share Value",
            aggfunc="first",
        )
        .sort_index()
        .tail(max(rows, 1))
    )
    if pivot.empty:
        return pivot
    formatted = pivot.map(lambda value: f"{value * 100:.1f}%")
    formatted = formatted.reset_index()
    formatted["Generated At"] = formatted["Generated At"].dt.strftime("%Y-%m-%d %H:%MZ")
    return formatted


def summarize_portfolio(history: pd.DataFrame, airlines: list[str]) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame()
    primary_sources = ["airline_config", "equipment_estimate", "unknown"]
    records = []
    for airline in airlines:
        latest, latest_ts = latest_snapshot(history, airline)
        if latest.empty or latest_ts is None:
            continue
        share_lookup = latest.set_index("Seat Source")["ASM Share Value"].to_dict()
        row = {
            "Airline": airline,
            "Last Updated": latest_ts.strftime("%Y-%m-%d %H:%MZ"),
        }
        for source in primary_sources:
            row[f"{source} %"] = f"{share_lookup.get(source, 0) * 100:.1f}%"
        other_share = 1.0 - sum(share_lookup.get(source, 0) for source in primary_sources)
        other_share = max(other_share, 0.0)
        row["other %"] = f"{other_share * 100:.1f}%"
        records.append(row)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records).sort_values("Airline").reset_index(drop=True)


def main():
    args = parse_args()
    history = load_history(args.input)
    generated_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    lines = [
        "# ASM Data Quality Dashboard",
        f"_Generated: {generated_ts}_",
        "",
    ]

    if history.empty:
        lines.append(f"No ASM summary history found at `{args.input}`.")
    else:
        airlines = args.airline or sorted(history["Airline"].unique())
        overview = summarize_portfolio(history, airlines)
        if not overview.empty:
            lines.append("## Portfolio Overview")
            lines.append(
                "Seat-source mix at the latest snapshot for each airline (higher config percentages indicate more reliable ASM)."
            )
            lines.append(df_to_markdown(overview))
            lines.append("")

        for airline in airlines:
            latest, latest_ts = latest_snapshot(history, airline)
            lines.append(f"## {airline}")
            if latest.empty:
                lines.append("_No snapshots have been recorded yet._")
                continue
            lines.append(f"_Last updated: {latest_ts.strftime('%Y-%m-%d %H:%MZ')}_")
            lines.append("")
            lines.append("### Seat Source Mix (latest)")
            columns = ["Seat Source", "Routes", "Valid ASM Routes", "Total Seats", "Total ASM", "ASM Share"]
            columns = [col for col in columns if col in latest.columns]
            lines.append(df_to_markdown(latest[columns]))
            lines.append("")
            trend = build_trend(history, airline, args.trend_rows)
            if not trend.empty:
                lines.append(f"### ASM Share Trend (last {len(trend)} snapshots)")
                lines.append(df_to_markdown(trend))
                lines.append("")
            else:
                lines.append("_Not enough history for a trend chart yet._")
                lines.append("")

    ensure_parent = args.output.parent
    ensure_parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote dashboard to {args.output}")


if __name__ == "__main__":
    main()
