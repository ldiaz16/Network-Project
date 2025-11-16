import argparse
from datetime import datetime, timezone
import os
import shutil
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.load_data import DataStore


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate Markdown/PDF reports showcasing airline comparisons and CBSA opportunities."
    )
    parser.add_argument(
        "--airline",
        action="append",
        help="Airline to include in the comparison (supply twice to override defaults).",
    )
    parser.add_argument(
        "--cbsa-airline",
        action="append",
        help="Additional airline to include in the CBSA opportunity showcase.",
    )
    parser.add_argument(
        "--cbsa-top-n",
        type=int,
        default=5,
        help="Top routes per airline for CBSA simulation.",
    )
    parser.add_argument(
        "--cbsa-suggestions",
        type=int,
        default=3,
        help="Maximum CBSA opportunities per top route.",
    )
    parser.add_argument(
        "--max-print",
        type=int,
        default=10,
        help="Rows per table in the report.",
    )
    parser.add_argument(
        "--output-markdown",
        type=Path,
        default=Path("reports/airline_showcase.md"),
        help="Destination markdown file.",
    )
    parser.add_argument(
        "--output-pdf",
        type=Path,
        help="Optional PDF path. Requires pandoc in PATH.",
    )
    parser.add_argument(
        "--asm-summary-log",
        type=Path,
        default=Path("reports/asm_summary_history.csv"),
        help="CSV file to append ASM accuracy snapshots.",
    )
    parser.add_argument(
        "--asm-estimate-threshold",
        type=float,
        default=0.4,
        help="Alert threshold for equipment-estimated ASM share (0-1).",
    )
    parser.add_argument(
        "--asm-unknown-threshold",
        type=float,
        default=0.1,
        help="Alert threshold for unknown seat-source ASM share (0-1).",
    )
    return parser.parse_args()


def ensure_parent_dir(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def _sanitize_cell(value):
    text = str(value)
    text = text.replace("\\", "\\\\")
    text = text.replace("|", "\\|")
    text = text.replace("\n", " ")
    return text


def df_to_markdown(df, columns=None, max_rows=10):
    if df is None or df.empty:
        return "_No data available._\n"
    subset = df[columns] if columns else df
    limited = subset.head(max_rows).fillna("—")
    headers = " | ".join(_sanitize_cell(col) for col in limited.columns)
    separator = " | ".join("---" for _ in limited.columns)
    rows = [
        " | ".join(_sanitize_cell(value) for value in row)
        for row in limited.to_numpy()
    ]
    table_lines = [f"| {headers} |", f"| {separator} |"]
    table_lines.extend(f"| {row} |" for row in rows)
    return "\n".join(table_lines) + "\n"


def persist_asm_summary(summary_df, airline_name, log_path):
    if log_path is None or summary_df is None or summary_df.empty:
        return
    ensure_parent_dir(log_path)
    timestamp = datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat(timespec="seconds")
    timestamp = timestamp.replace("+00:00", "Z")
    snapshot = summary_df.copy()
    snapshot.insert(0, "Generated At", timestamp)
    snapshot.insert(0, "Airline", airline_name)
    log_exists = log_path.exists()
    mode = "a" if log_exists else "w"
    snapshot.to_csv(log_path, mode=mode, index=False, header=not log_exists)


def build_airline_package(ds, query, announce=True):
    routes_df, meta = ds.select_airline_routes(query, verbose=announce)
    processed = ds.process_routes(routes_df)
    cost = ds.cost_analysis(processed)
    network = ds.build_network(processed)
    stats = ds.analyze_network(network)
    return {
        "name": meta["Airline"],
        "normalized": meta["Airline (Normalized)"],
        "routes": routes_df,
        "processed": processed,
        "cost": cost,
        "network_stats": stats,
    }


def format_network_section(package):
    stats = package["network_stats"]
    return dedent(
        f"""
        ### {package['name']}

        * Routes analyzed: {len(package['processed'])}
        * Airports served: {stats['Number of Aiports Flown To']}
        * Total routes: {stats['Number of Routes Flown']}
        * Top hubs: {stats['Top 5 Hubs']}
        """
    ).strip()


def build_report(args, packages, cbsa_targets, ds):
    lines = ["# Airline Route Optimizer Report"]

    names = ", ".join(pkg["name"] for pkg in packages)
    lines.append(dedent(
        f"""
        _Auto-generated using scripts/report.py._

        **Airlines showcased:** {names}
        """
    ).strip())

    lines.append("## Network Snapshots")
    for pkg in packages:
        lines.append(format_network_section(pkg))

    lines.append("## ASM Accuracy Snapshot")
    asm_columns = [
        "Seat Source",
        "Routes",
        "Valid ASM Routes",
        "Total Seats",
        "Total ASM",
        "ASM Share",
    ]
    for pkg in packages:
        summary = ds.summarize_asm_sources(pkg["cost"])
        lines.append(f"### {pkg['name']}")
        lines.append(df_to_markdown(summary, asm_columns, args.max_print))
        persist_asm_summary(summary, pkg["name"], args.asm_summary_log)
        alerts = ds.detect_asm_alerts(summary, args.asm_estimate_threshold, args.asm_unknown_threshold)
        if alerts:
            lines.append("**Data quality alerts**")
            lines.extend(f"- {alert}" for alert in alerts)
        else:
            lines.append("_ASM data quality looks healthy._")

    if len(packages) == 2:
        lines.append("## Competing Route Overlap\n")
        comparison = ds.find_competing_routes(packages[0]["cost"], packages[1]["cost"])

        def _candidate_columns(package):
            bases = []
            for key in ("name", "normalized"):
                value = package.get(key)
                if isinstance(value, str) and value:
                    bases.append(value)
            return bases

        def _find_first(df, bases, suffix=""):
            for base in bases:
                candidate = f"{base}{suffix}"
                if candidate in df.columns:
                    return candidate
            return None

        base_columns = ["Source", "Dest"]
        airlines_a = _candidate_columns(packages[0])
        airlines_b = _candidate_columns(packages[1])

        dynamic_cols = []
        for bases in (airlines_a, airlines_b):
            col = _find_first(comparison, bases)
            if col:
                dynamic_cols.append(col)
        for bases in (airlines_a, airlines_b):
            col = _find_first(comparison, bases, "_Aircraft")
            if col:
                dynamic_cols.append(col)

        comp_cols = base_columns + dynamic_cols
        existing_cols = [col for col in comp_cols if col in comparison.columns]
        lines.append(df_to_markdown(comparison, existing_cols or None, args.max_print))

    lines.append("## CBSA Opportunity Highlights")
    for key in cbsa_targets:
        pkg = cbsa_targets[key]
        lines.append(f"### {pkg['name']}")
        simulation = ds.simulate_cbsa_route_opportunities(
            pkg["cost"],
            top_n=args.cbsa_top_n,
            max_suggestions_per_route=args.cbsa_suggestions,
        )

        best_cols = [
            "Route",
            "Source CBSA Name",
            "Destination CBSA Name",
            "ASM",
            "Total Seats",
            "Performance Score",
        ]
        lines.append("**Top CBSA corridors**")
        lines.append(df_to_markdown(simulation["best_routes"], best_cols, args.max_print))

        opportunity_cols = [
            "Proposed Source",
            "Proposed Destination",
            "Source CBSA",
            "Destination CBSA",
            "Reference Route",
            "Opportunity Score",
        ]
        lines.append("**Suggested CBSA opportunities**")
        lines.append(df_to_markdown(simulation["suggested_routes"], opportunity_cols, args.max_print))

    lines.append("## Next Steps")
    lines.append(dedent(
        """
        * Run `python3 main.py --airline AIRLINE1 --airline AIRLINE2` for interactive comparisons.
        * Seed CBSA data via `python3 main.py --build-cbsa-cache`.
        * Use `scripts/showcase.py` for a quick CLI preview.
        """
    ).strip())

    return "\n\n".join(lines) + "\n"


def convert_markdown_to_pdf(markdown_path: Path, pdf_path: Path):
    pandoc = shutil.which("pandoc")
    if pandoc is None:
        print("⚠️  pandoc is not installed; skipping PDF generation.", file=sys.stderr)
        return False

    cmd = [pandoc, str(markdown_path), "-o", str(pdf_path)]
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as exc:
        print(f"⚠️  Failed to generate PDF via pandoc: {exc}", file=sys.stderr)
        return False


def main():
    args = parse_args()
    ds = DataStore()
    ds.load_data()

    compare_queries = args.airline or ["Delta Air Lines", "American Airlines"]
    packages = []
    package_registry = {}

    for query in compare_queries[:2]:
        pkg = build_airline_package(ds, query, announce=True)
        packages.append(pkg)
        package_registry[pkg["normalized"]] = pkg

    if args.cbsa_airline:
        for query in args.cbsa_airline:
            pkg = build_airline_package(ds, query, announce=True)
            package_registry[pkg["normalized"]] = pkg

    cbsa_targets = package_registry
    report_markdown = build_report(args, packages, cbsa_targets, ds)

    ensure_parent_dir(args.output_markdown)
    args.output_markdown.write_text(report_markdown, encoding="utf-8")
    print(f"Markdown report written to {args.output_markdown}")

    if args.output_pdf:
        ensure_parent_dir(args.output_pdf)
        success = convert_markdown_to_pdf(args.output_markdown, args.output_pdf)
        if success:
            print(f"PDF report written to {args.output_pdf}")


if __name__ == "__main__":
    main()
