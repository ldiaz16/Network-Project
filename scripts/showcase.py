import argparse
import os
import sys
from textwrap import dedent

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.load_data import DataStore


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
    return parser.parse_args()


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


def build_airline_package(ds, query, announce=True):
    routes_df, meta = ds.select_airline_routes(query, verbose=announce)
    processed = ds.process_routes(routes_df)
    cost = ds.cost_analysis(processed)
    stats = ds.analyze_network(ds.build_network(processed))
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


def showcase_cbsa(ds, package, args):
    print_section(f"CBSA Opportunity Simulation — {package['name']}")
    simulation = ds.simulate_cbsa_route_opportunities(
        package["cost"],
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
    print("Top-performing CBSA corridors:")
    print(format_table(simulation["best_routes"], best_cols, args.max_print))

    opportunity_cols = [
        "Proposed Source",
        "Proposed Destination",
        "Source CBSA",
        "Destination CBSA",
        "Reference Route",
        "Opportunity Score",
    ]
    print("Look-alike CBSA opportunities:")
    print(format_table(simulation["suggested_routes"], opportunity_cols, args.max_print))


def main():
    args = parse_args()
    ds = DataStore()
    ds.load_data()

    default_compare = ["Delta Air Lines", "American Airlines"]
    compare_queries = args.airline or default_compare

    packages = []
    package_registry = {}

    for query in compare_queries[:2]:
        pkg = build_airline_package(ds, query, announce=True)
        packages.append(pkg)
        package_registry[pkg["normalized"]] = pkg

    print_section("Network Snapshots")
    for pkg in packages:
        print(f"{pkg['name']}:")
        print(show_network_stats(pkg))

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
