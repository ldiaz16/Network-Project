import argparse
from datetime import datetime, timezone
import os
import sys
from pathlib import Path
from textwrap import dedent

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.load_data import DataStore

QUARTER_RANGE = ((2022, 2), (2025, 2))


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Pretty-print the key capabilities of the airline route optimizer:\n"
            "- competing-route detection for two airlines\n"
            "- network stats\n"
            "- CBSA-driven opportunity simulation"
        )
    )
    parser.add_argument(
        "--airline",
        action="append",
        help="Airline to highlight in the comparison. Supply twice to override both defaults.",
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
        help="Number of top routes to seed the CBSA simulation.",
    )
    parser.add_argument(
        "--cbsa-suggestions",
        type=int,
        default=3,
        help="Maximum CBSA-similar routes to suggest per seed route.",
    )
    parser.add_argument(
        "--max-print",
        type=int,
        default=5,
        help="Number of rows to print for each highlighted table.",
    )
    parser.add_argument(
        "--asm-summary-log",
        type=Path,
        default=Path("reports/asm_summary_history.csv"),
        help="CSV file to append ASM summary snapshots.",
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
    parser.add_argument(
        "--top-airlines",
        type=int,
        default=5,
        help="When no --airline is supplied, highlight this many carriers ranked by passenger volume.",
    )
    return parser.parse_args()


def _top_airline_codes_by_passengers(ds, limit):
    try:
        max_limit = max(0, int(limit))
    except (TypeError, ValueError):
        return []
    if max_limit <= 0:
        return []
    routes = getattr(ds, "routes", None)
    if routes is None or routes.empty or "Airline Code" not in routes.columns:
        return []
    metric = "Passengers" if "Passengers" in routes.columns else "Total"
    if metric not in routes.columns:
        return []
    grouped = (
        routes[["Airline Code", metric]]
        .dropna(subset=["Airline Code"])
        .copy()
        .groupby("Airline Code", as_index=False)[metric]
        .sum()
        .sort_values(metric, ascending=False)
    )
    codes = grouped["Airline Code"].fillna("").astype(str).str.strip().tolist()
    return [code for code in codes if code][:max_limit]


def print_section(title, body=""):
    border = "=" * len(title)
    print(f"\n{border}\n{title}\n{border}")
    if body:
        print(body)


def format_table(df, columns=None, max_rows=5):
    if df is None or df.empty:
        return "  (no data)\n"
    subset = df[columns] if columns else df
    limited = subset.head(max_rows)
    return dedent(limited.to_string(index=False)) + "\n"


def persist_asm_summary(summary_df, airline_name, log_path):
    if log_path is None or summary_df is None or summary_df.empty:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
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
    stats = ds.analyze_network(
        ds.build_network(processed),
        meta.get("Airline (Normalized)") or meta.get("Airline"),
        processed_routes=processed,
    )
    package = {
        "name": meta["Airline"],
        "normalized": meta["Airline (Normalized)"],
        "routes": routes_df,
        "processed": processed,
        "cost": cost,
        "network_stats": stats,
    }
    return package


def show_network_stats(package):
    stats = package["network_stats"]
    lines = [
        f"  Routes analyzed: {len(package['processed'])}",
        f"  Airports served: {stats['Number of Aiports Flown To']}",
        f"  Total routes (edges): {stats['Number of Routes Flown']}",
        f"  Top hubs: {stats['Top 5 Hubs']}",
    ]
    return "\n".join(lines) + "\n"


def show_asm_summary(ds, package, max_rows):
    summary = ds.summarize_asm_sources(package["cost"])
    columns = [
        "Seat Source",
        "Routes",
        "Valid ASM Routes",
        "Total Seats",
        "Total ASM",
        "ASM Share",
    ]
    return summary, format_table(summary, columns, max_rows)


def showcase_cbsa(ds, package, args):
    print_section(f"CBSA Opportunity Simulation — {package['name']}")
    simulation = ds.simulate_cbsa_route_opportunities(
        package["cost"],
        top_n=args.cbsa_top_n,
        max_suggestions_per_route=args.cbsa_suggestions,
    )
    best_cols = [
        "Route",
        "ASM Share",
        "Performance Score",
        "Route Strategy Baseline",
        "Competition Score",
        "Route Maturity Score",
        "Yield Proxy Score",
        "Total Seats",
        "Distance (miles)",
        "Seats per Mile",
        "Route Rationale",
    ]
    print("Top-performing CBSA corridors:")
    print(format_table(simulation["best_routes"], best_cols, args.max_print))

    opportunity_cols = [
        "Proposed Source",
        "Proposed Destination",
        "Reference Route",
        "Distance Similarity",
        "Estimated Distance (miles)",
        "Opportunity Score",
        "Rationale",
    ]
    print("Look-alike CBSA opportunities:")
    print(format_table(simulation["suggested_routes"], opportunity_cols, args.max_print))


def main():
    args = parse_args()
    ds = DataStore(quarter_range=QUARTER_RANGE)
    ds.load_data()

    fallback_queries = ["Delta Air Lines", "American Airlines"]
    limit = max(0, int(args.top_airlines))
    candidate_queries = args.airline or _top_airline_codes_by_passengers(ds, limit)
    if not candidate_queries:
        candidate_queries = fallback_queries
    if not args.airline and limit > 0:
        candidate_queries = candidate_queries[:limit]
    compare_queries = candidate_queries


    packages = []
    package_registry = {}

    for query in compare_queries:
        pkg = build_airline_package(ds, query, announce=True)
        packages.append(pkg)
        package_registry[pkg["normalized"]] = pkg

    print_section("Network Snapshots")
    for pkg in packages:
        print(f"{pkg['name']}:")
        print(show_network_stats(pkg))

    print_section("ASM Accuracy Snapshot")
    for pkg in packages:
        print(f"{pkg['name']}:")
        summary, table = show_asm_summary(ds, pkg, args.max_print)
        print(table)
        persist_asm_summary(summary, pkg["name"], args.asm_summary_log)
        alerts = ds.detect_asm_alerts(summary, args.asm_estimate_threshold, args.asm_unknown_threshold)
        if alerts:
            print("  Alerts:")
            for alert in alerts:
                print(f"    - {alert}")
        else:
            print("  Alerts: none")

    if len(packages) == 2:
        print_section("Competing Route Overlap")
        comp = ds.find_competing_routes(packages[0]["cost"], packages[1]["cost"])
        comp_cols = [
            "Source",
            "Dest",
            packages[0]["normalized"],
            packages[1]["normalized"],
        ]
        existing_cols = [col for col in comp_cols if col in comp.columns]
        print(format_table(comp, existing_cols or None, args.max_print))

    cbsa_targets = set(pkg["normalized"] for pkg in packages)
    if args.cbsa_airline:
        for query in args.cbsa_airline:
            pkg = build_airline_package(ds, query, announce=True)
            key = pkg["normalized"]
            cbsa_targets.add(key)
            if key not in package_registry:
                package_registry[key] = pkg

    print_section("CBSA Opportunity Highlights")
    sorted_targets = sorted(cbsa_targets)
    for key in sorted_targets:
        showcase_cbsa(ds, package_registry[key], args)

    print_section("Next Steps", dedent("""\
        • Use `python3 main.py --airline AIRLINE1 --airline AIRLINE2` to deep-dive two carriers.
        • Seed CBSA data ahead of time with `python3 main.py --build-cbsa-cache`.
        • Export CBSA reports via `python3 main.py --cbsa-save-dir data/reports`.
    """))


if __name__ == "__main__":
    main()
