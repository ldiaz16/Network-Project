import argparse
from pathlib import Path

from src.load_data import DataStore
from backend.app import app as flask_app

# Expose Flask app for serverless platforms expecting `app`.
app = flask_app


def parse_args():
    parser = argparse.ArgumentParser(
        description="Airline route optimizer with CBSA-aware simulations."
    )
    parser.add_argument(
        "--airline",
        action="append",
        help="Airline to include in the two-airline comparison; specify twice for both competitors."
    )
    parser.add_argument(
        "--skip-comparison",
        action="store_true",
        help="Skip the two-airline comparison workflow and only run CBSA simulations."
    )
    parser.add_argument(
        "--cbsa-airline",
        action="append",
        help="Airline to evaluate for CBSA opportunity simulation (in addition to any comparison airlines)."
    )
    parser.add_argument(
        "--cbsa-top-n",
        type=int,
        default=5,
        help="Number of top-performing routes to seed the CBSA simulation."
    )
    parser.add_argument(
        "--cbsa-suggestions",
        type=int,
        default=3,
        help="Maximum number of CBSA-similar opportunities suggested per top route."
    )
    parser.add_argument(
        "--cbsa-save-dir",
        type=str,
        help="Optional directory to persist CBSA best-route and suggestion tables as CSV files."
    )
    parser.add_argument(
        "--build-cbsa-cache",
        action="store_true",
        help="Batch annotate airports to prime the CBSA cache (default country: United States)."
    )
    parser.add_argument(
        "--cbsa-cache-limit",
        type=int,
        help="Maximum number of airports to process while building the CBSA cache."
    )
    parser.add_argument(
        "--cbsa-cache-chunk-size",
        type=int,
        default=200,
        help="Number of airports to annotate per batch when building the CBSA cache."
    )
    parser.add_argument(
        "--cbsa-cache-country",
        action="append",
        help="Country to include when building the CBSA cache. Repeat for multiple countries."
    )
    return parser.parse_args()


def slugify(value):
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in value)
    cleaned = "_".join(filter(None, cleaned.split("_")))
    return cleaned or "airline"


def build_airline_package(data_storage, routes_df):
    if routes_df.empty:
        raise ValueError("Airline has no routes available for analysis.")
    processed_df = data_storage.process_routes(routes_df)
    cost_df = data_storage.cost_analysis(processed_df)
    airline_name = routes_df["Airline"].iloc[0]
    normalized_name = routes_df["Airline (Normalized)"].iloc[0]
    return {
        "name": airline_name,
        "normalized": normalized_name,
        "routes": routes_df,
        "processed": processed_df,
        "cost": cost_df
    }


def persist_cbsa_results(save_dir, airline_name, best_routes, suggestions):
    if not save_dir:
        return
    output_dir = Path(save_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = slugify(airline_name)
    if not best_routes.empty:
        best_path = output_dir / f"{slug}_cbsa_best_routes.csv"
        best_routes.to_csv(best_path, index=False)
        print(f"Saved best-route table for {airline_name} to {best_path}")
    if not suggestions.empty:
        suggestions_path = output_dir / f"{slug}_cbsa_suggestions.csv"
        suggestions.to_csv(suggestions_path, index=False)
        print(f"Saved CBSA opportunity table for {airline_name} to {suggestions_path}")


def run_cbsa_simulation(data_storage, airline_name, airline_cost_df, args):
    simulation = data_storage.simulate_cbsa_route_opportunities(
        airline_cost_df,
        top_n=args.cbsa_top_n,
        max_suggestions_per_route=args.cbsa_suggestions
    )

    print(f"\nTop CBSA-aligned routes for {airline_name}:")
    best_routes = simulation["best_routes"]
    if best_routes.empty:
        print("No enriched route data available.")
    else:
        cbsa_display_columns = [
            "Route",
            "Performance Score",
            "Route Strategy Baseline",
            "Competition Score",
            "Route Maturity Score",
            "Yield Proxy Score",
            "ASM",
            "Total Seats",
            "Distance (miles)",
            "Seats per Mile",
            "Route Rationale",
        ]
        available_columns = [col for col in cbsa_display_columns if col in best_routes.columns]
        print(best_routes[available_columns].to_string(index=False))

    suggestions = simulation["suggested_routes"]
    if suggestions.empty:
        print("No CBSA-similar opportunities identified.")
    else:
        display_columns = [
            "Proposed Source",
            "Proposed Destination",
            "Reference Route",
            "Distance Similarity",
            "Estimated Distance (miles)",
            "Opportunity Score",
            "Rationale"
        ]
        print("Suggested CBSA-similar opportunities:")
        print(suggestions[display_columns].head(10).to_string(index=False))

    persist_cbsa_results(args.cbsa_save_dir, airline_name, best_routes, suggestions)


def main():
    args = parse_args()
    data_storage = DataStore()
    data_storage.load_data()

    if args.build_cbsa_cache:
        target_countries = args.cbsa_cache_country or ["United States"]
        print(f"Building CBSA cache for countries: {', '.join(target_countries)}")
        processed = data_storage.build_cbsa_cache(
            countries=target_countries,
            limit=args.cbsa_cache_limit,
            chunk_size=args.cbsa_cache_chunk_size
        )
        print(f"CBSA cache build complete. Processed {processed} airports.")

    def package_from_query(query, announce=True):
        routes_df, _ = data_storage.select_airline_routes(query, verbose=announce)
        return build_airline_package(data_storage, routes_df)

    comparison_packages = []
    if args.airline:
        for query in args.airline:
            comparison_packages.append(package_from_query(query))

    while len(comparison_packages) < 2 and not args.skip_comparison:
        routes_df = data_storage.user_airline()
        comparison_packages.append(build_airline_package(data_storage, routes_df))

    if not args.skip_comparison and len(comparison_packages) < 2:
        raise SystemExit("Two airlines are required for comparison. Provide them via --airline or interactively.")

    cbsa_registry = {}

    def register_cbsa(pkg):
        cbsa_registry[pkg["normalized"]] = {
            "name": pkg["name"],
            "cost": pkg["cost"]
        }

    for pkg in comparison_packages:
        register_cbsa(pkg)

    if args.cbsa_airline:
        for query in args.cbsa_airline:
            pkg = package_from_query(query)
            register_cbsa(pkg)

    if not args.skip_comparison:
        airline_x_pkg, airline_y_pkg = comparison_packages[:2]
        airline_x_df = airline_x_pkg["processed"]
        airline_y_df = airline_y_pkg["processed"]
        airline_x_cost_df = airline_x_pkg["cost"]
        airline_y_cost_df = airline_y_pkg["cost"]

        competing_routes = data_storage.find_competing_routes(airline_x_cost_df, airline_y_cost_df)
        print(competing_routes.head(10))

        airline_x_network = data_storage.build_network(airline_x_df)
        airline_y_network = data_storage.build_network(airline_y_df)

        airline_x_stats = data_storage.analyze_network(airline_x_network)
        airline_y_stats = data_storage.analyze_network(airline_y_network)

        print("Network analysis for Airlines")
        print(airline_x_pkg["name"], "Network:")
        for key, value in airline_x_stats.items():
            print(f"{key}: {value}")
        print("\n")
        print(airline_y_pkg["name"], "Network:")
        for key, value in airline_y_stats.items():
            print(f"{key}: {value}")

    if not cbsa_registry:
        if args.build_cbsa_cache:
            print("\nCBSA cache build finished. No CBSA simulation targets were specified.")
            return
        print("\nNo CBSA simulation targets were provided. Use --cbsa-airline or --airline to specify at least one airline.")
        return

    for record in cbsa_registry.values():
        run_cbsa_simulation(
            data_storage,
            record["name"],
            record["cost"],
            args
        )


if __name__ == "__main__":
    main()
