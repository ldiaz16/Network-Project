import sys
import json
from pathlib import Path
import requests
import pandas as pd
from data.airlines import normalize_name
import networkx as nx
from rapidfuzz import process, fuzz # type: ignore
from geopy.distance import geodesic
from data.aircraft_config import AIRLINE_SEAT_CONFIG
from collections import defaultdict
from .data_utils import filter_codeshare_routes

DATA_DIR_ROOT = Path(__file__).resolve().parents[1] / "data"
RESOURCES_DIR = Path(__file__).resolve().parents[1] / "resources"
LOAD_FACTOR_BENCHMARKS = {
    "delta air lines": 0.86,
    "delta": 0.86,
    "dl": 0.86,
    "united airlines": 0.844,
    "united": 0.844,
    "ua": 0.844,
    "jetblue airways corporation": 0.851,
    "jetblue airways": 0.851,
    "jetblue": 0.851,
    "b6": 0.851,
}

GENERIC_SEAT_GUESSES = {
    "100": 100,  # Fokker 100 / similar regional jets
    "318": 120,
    "319": 136,
    "320": 162,
    "321": 191,
    "332": 223,
    "333": 281,
    "330": 275,
    "343": 275,
    "359": 306,
    "717": 110,
    "720": 149,
    "733": 130,
    "735": 118,
    "737": 143,
    "738": 175,
    "739": 180,
    "73C": 143,
    "73G": 143,
    "73H": 160,
    "73W": 143,
    "752": 199,
    "753": 234,
    "757": 199,
    "763": 226,
    "764": 238,
    "772": 314,
    "777": 314,
    "77W": 368,
    "787": 248,
    "788": 242,
    "789": 285,
    "A20N": 188,
    "AT7": 70,
    "ATR": 70,
    "CR2": 50,
    "CR7": 65,
    "CR9": 70,
    "CRJ": 50,
    "DH2": 37,
    "DH3": 50,
    "DH4": 76,
    "DH8": 50,
    "E70": 70,
    "E75": 76,
    "E90": 100,
    "E95": 110,
    "ER3": 50,
    "ER4": 50,
    "ERD": 50,
    "ERJ": 50,
    "M80": 140,
    "M83": 140,
    "M88": 150,
    "SF3": 34,
    "SU9": 98,
}

class DataStore:
    def __init__(self, data_dir=None):
        base_dir = Path(data_dir) if data_dir else DATA_DIR_ROOT
        self.data_dir = base_dir
        self.resources_dir = RESOURCES_DIR
        self.routes = None
        self.airlines = None
        self.airports = None
        self.aircraft_config = None
        self.equipment_capacity_lookup = {}
        self._equipment_lookup_keys = []
        self.airports_metadata = None
        self.cbsa_cache = {}
        self._cbsa_cache_dirty = False
        self.cbsa_lookup = None
        self.cbsa_cache_path = self.data_dir / "cbsa_cache.json"
        self._load_cbsa_cache()
        self._route_total_asm_cache = {}
        self.codeshare_overrides = self._load_codeshare_overrides()
        self.operational_metrics = self._load_operational_metrics()

    def load_data(self):
        """Load data from CSV files into DataFrames and preprocess them."""
        ## Create DataFrames for each dataset
        self.routes = pd.read_csv(self.data_dir / "routes.dat", header=None)
        self.airlines = pd.read_csv(self.data_dir / "airlines.dat", header=None)
        self.airports = pd.read_csv(self.data_dir / "airports.dat", header=None)
        self.cbsa = pd.read_csv(self.data_dir / "cbsa.csv", skiprows=2)
        self.cbsa_lookup = self.cbsa[[
            "CBSA Title",
            "County/County Equivalent",
            "State Name",
            "CBSA Code"
        ]].copy()
        self.cbsa_lookup["county_key"] = self.cbsa_lookup["County/County Equivalent"].astype(str).str.strip().str.lower()
        self.cbsa_lookup["state_key"] = self.cbsa_lookup["State Name"].astype(str).str.strip().str.lower()

        
        # Drop the first column which is not needed
        self.airlines.drop(columns=[0], inplace=True) 

        ## Add column names to each DataFrame
        self.routes.columns = [
            "Airline Code", "IDK", "Source airport", "Source airport ID",
            "Destination airport", "Destination airport ID", "Codeshare", "Stops", "Equipment"
        ]
        self.routes = filter_codeshare_routes(self.routes)
        self.airlines.columns = [
            "Airline", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active"
        ]
        self.airports.columns = [
            "Airport ID", "Name", "City", "Country", "IATA", "ICAO",
            "Latitude", "Longitude", "Altitude", "Timezone", "DST", "Tz", "Type", "Source"
        ]

        ## Normalize the airline names in the airlines DataFrame, 
        ## creating a new column.
        self.airlines["Airline (Normalized)"] = self.airlines["Airline"].apply(normalize_name)

        # Build airport metadata lazily to avoid excessive API calls
        self.airports_metadata = None

        ## Remove rows with missing or empty IATA codes (Non-operational airlines)
        self.airlines = self.airlines[self.airlines["IATA"].apply(lambda x: isinstance(x, str) and x.strip() != "")]
        self.aircraft_config = self.convert_aircraft_config_to_df(AIRLINE_SEAT_CONFIG)
        self.equipment_capacity_lookup = self._build_equipment_capacity_lookup(self.aircraft_config)
        ## print(self.aircraft_config.head())
        

    def convert_aircraft_config_to_df(self, config_dict):
        """Convert the AIRLINE_SEAT_CONFIG dictionary to a DataFrame."""
        data = []
        for airline, aircrafts in config_dict.items():
            for aircraft, cabin_classes in aircrafts.items():
                for cabin_code, seat_count in cabin_classes.items():
                    data.append({
                        "Airline": normalize_name(airline),
                        "Aircraft": aircraft,
                        "Cabin Class": cabin_code,
                        "Seats": seat_count
                    })
        df_long = pd.DataFrame(data)
        df_wide = df_long.pivot_table(
            index=["Airline", "Aircraft"],
            columns="Cabin Class",
            values="Seats",
            fill_value=0
        ).reset_index()
        desired_order = ["Airline", "Aircraft", "Y", "W", "J", "F", "Total"]
        df = df_wide[desired_order] 
        seat_columns = ["Y", "W", "J", "F", "Total"]
        df[seat_columns] = df[seat_columns].astype(int)
        return df


    def _load_cbsa_cache(self):
        """Load cached CBSA lookups from disk if available."""
        try:
            if self.cbsa_cache_path.exists():
                with self.cbsa_cache_path.open("r", encoding="utf-8") as cache_file:
                    data = json.load(cache_file)
                    if isinstance(data, dict):
                        self.cbsa_cache = data
        except Exception:
            # Ignore cache loading failures and start fresh
            self.cbsa_cache = {}


    def _save_cbsa_cache(self):
        """Persist CBSA cache to disk when new values were recorded."""
        if not self._cbsa_cache_dirty:
            return
        try:
            self.cbsa_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self.cbsa_cache_path.open("w", encoding="utf-8") as cache_file:
                json.dump(self.cbsa_cache, cache_file)
            self._cbsa_cache_dirty = False
        except Exception:
            # Silently skip persistence issues; cache is an optimization only
            pass

    def _load_codeshare_overrides(self):
        """
        Load manual codeshare overrides that flag marketing-only segments.

        The file (data/codeshare_overrides.json) can specify either whole airports
        or explicit source/destination pairs to remove for a given airline.
        """
        overrides_path = self.data_dir / "codeshare_overrides.json"
        if not overrides_path.exists():
            return {}
        try:
            with overrides_path.open("r", encoding="utf-8") as raw_file:
                payload = json.load(raw_file)
        except Exception:
            return {}

        normalized = {}
        for airline_name, config in (payload or {}).items():
            if not isinstance(config, dict):
                continue
            normalized_name = normalize_name(airline_name)
            if not normalized_name:
                continue

            blocked_airports = {
                str(code).strip().upper()
                for code in config.get("blocked_airports", [])
                if isinstance(code, str) and code.strip()
            }

            blocked_pairs = set()
            for entry in config.get("blocked_pairs", []):
                if isinstance(entry, dict):
                    source = entry.get("source")
                    destination = entry.get("destination")
                elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                    source, destination = entry[0], entry[1]
                else:
                    continue
                if not isinstance(source, str) or not isinstance(destination, str):
                    continue
                source = source.strip().upper()
                destination = destination.strip().upper()
                if not source or not destination:
                    continue
                if source <= destination:
                    blocked_pairs.add(f"{source}||{destination}")
                else:
                    blocked_pairs.add(f"{destination}||{source}")

            normalized[normalized_name] = {
                "blocked_airports": blocked_airports,
                "blocked_pairs": blocked_pairs,
            }

        return normalized

    def _apply_codeshare_overrides(self, routes_df, normalized_airline):
        """
        Drop known codeshare-only segments for the provided airline if overrides exist.
        """
        if routes_df is None or routes_df.empty:
            return routes_df
        normalized_key = normalize_name(normalized_airline) if normalized_airline else None
        overrides = None
        if normalized_key and normalized_key in self.codeshare_overrides:
            overrides = self.codeshare_overrides[normalized_key]
        elif normalized_airline in self.codeshare_overrides:
            overrides = self.codeshare_overrides[normalized_airline]
        if not overrides:
            return routes_df

        blocked_airports = overrides.get("blocked_airports") or set()
        blocked_pairs = overrides.get("blocked_pairs") or set()
        if not blocked_airports and not blocked_pairs:
            return routes_df

        source_codes = routes_df["Source airport"].fillna("").astype(str).str.upper()
        dest_codes = routes_df["Destination airport"].fillna("").astype(str).str.upper()
        mask = pd.Series(True, index=routes_df.index)

        if blocked_airports:
            airport_mask = source_codes.isin(blocked_airports) | dest_codes.isin(blocked_airports)
            mask &= ~airport_mask

        if blocked_pairs:
            left = source_codes.where(source_codes <= dest_codes, dest_codes)
            right = dest_codes.where(source_codes <= dest_codes, source_codes)
            pair_keys = left + "||" + right
            mask &= ~pair_keys.isin(blocked_pairs)

        if mask.all():
            return routes_df
        return routes_df.loc[mask].copy()

    def _load_operational_metrics(self):
        """Load airline-level operational metrics (e.g., load factor averages)."""
        metrics_path = self.resources_dir / "airline_operational_metrics.json"
        if not metrics_path.exists():
            return {}
        try:
            with metrics_path.open("r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except Exception:
            return {}

        normalized = {}
        for airline_label, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            normalized_name = normalize_name(airline_label)
            entry = payload.copy()
            entry["airline_label"] = airline_label
            normalized[normalized_name] = entry
        return normalized

    def _normalize_load_factor(self, value, midpoint=0.82, spread=0.14):
        """Convert an absolute load factor into a 0-1 pressure scale."""
        try:
            load_factor = float(value)
        except (TypeError, ValueError):
            return None
        lower_bound = midpoint - (spread / 2)
        upper_bound = midpoint + (spread / 2)
        if upper_bound <= lower_bound:
            return None
        scaled = (load_factor - lower_bound) / (upper_bound - lower_bound)
        return max(0.0, min(1.0, scaled))

    def get_route_total_asm(self, route_pairs):
        """
        Return a mapping of (source, destination) pairs to the total ASM generated by
        all airlines that operate the route. Results are cached to avoid redundant work.
        """
        if not route_pairs:
            return {}
        if self.routes is None or self.airlines is None or self.airports is None:
            return {}

        normalized = []
        seen = set()
        for pair in route_pairs:
            if not isinstance(pair, tuple) or len(pair) != 2:
                continue
            source, dest = pair
            if not isinstance(source, str) or not isinstance(dest, str):
                continue
            key = (source, dest)
            if key not in seen:
                seen.add(key)
                normalized.append(key)

        if not normalized:
            return {}

        missing = [pair for pair in normalized if pair not in self._route_total_asm_cache]
        if missing:
            computed = self._compute_route_total_asm(missing)
            self._route_total_asm_cache.update(computed)

        return {pair: self._route_total_asm_cache.get(pair) for pair in normalized}

    def _compute_route_total_asm(self, route_pairs):
        """Heavy lifting for get_route_total_asm; expects validated route pairs."""
        pair_set = {(src, dest) for src, dest in route_pairs if src and dest}
        if not pair_set:
            return {pair: None for pair in route_pairs}

        source_filter = {src for src, _ in pair_set}
        dest_filter = {dest for _, dest in pair_set}

        subset = self.routes[
            self.routes["Source airport"].isin(source_filter)
            & self.routes["Destination airport"].isin(dest_filter)
        ].copy()

        if subset.empty:
            return {pair: 0.0 for pair in pair_set}

        if "Codeshare" in subset.columns:
            codeshare_mask = (
                subset["Codeshare"]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.upper()
                .replace({"\\N": ""})
            )
            subset = subset.loc[codeshare_mask != "Y"].copy()
            if subset.empty:
                return {pair: 0.0 for pair in pair_set}

        subset["__pair_key"] = list(zip(subset["Source airport"], subset["Destination airport"]))
        subset = subset[subset["__pair_key"].isin(pair_set)].copy()
        subset.drop(columns=["__pair_key"], inplace=True)

        if subset.empty:
            return {pair: 0.0 for pair in pair_set}

        airline_cols = ["IATA", "Airline", "Airline (Normalized)"]
        airline_lookup = (
            self.airlines[airline_cols]
            if all(col in self.airlines.columns for col in airline_cols)
            else None
        )
        if airline_lookup is not None:
            subset = subset.merge(
                airline_lookup,
                left_on="Airline Code",
                right_on="IATA",
                how="left"
            )
        else:
            subset["Airline"] = subset["Airline Code"]
            subset["Airline (Normalized)"] = subset["Airline Code"].str.lower()

        processed = self.process_routes(subset)
        if processed.empty:
            return {pair: 0.0 for pair in pair_set}

        cost = self.cost_analysis(processed, include_strategy_baseline=False)
        valid = cost[cost.get("ASM Valid", False).fillna(False)]
        if valid.empty:
            return {pair: 0.0 for pair in pair_set}

        aggregated = (
            valid.groupby(["Source airport", "Destination airport"], dropna=False)["ASM"]
            .sum(min_count=1)
            .reset_index()
        )

        totals = {pair: 0.0 for pair in pair_set}
        for _, row in aggregated.iterrows():
            key = (row["Source airport"], row["Destination airport"])
            totals[key] = float(row["ASM"]) if pd.notna(row["ASM"]) else 0.0
        return totals

    def evaluate_route_opportunity(self, airline_query, source_airport, dest_airport, seat_demand=None):
        """
        Provide a simple go/no-go recommendation for a proposed route based on:
        - Competition level on the O&D
        - Market ASM vs. airline median ASM (depth)
        - Distance fit vs. airline median stage length
        - Hub alignment and analog demand proxies to build a dynamic playbook
        """
        if not airline_query or not source_airport or not dest_airport:
            raise ValueError("Airline, source, and destination are required.")

        # Validate airports
        airports_lookup = self.airports.set_index("IATA") if self.airports is not None else pd.DataFrame()
        src_row = airports_lookup.loc[source_airport] if source_airport in airports_lookup.index else None
        dst_row = airports_lookup.loc[dest_airport] if dest_airport in airports_lookup.index else None
        if src_row is None or dst_row is None:
            raise ValueError("Source or destination airport not found.")

        # Pull airline data to derive medians
        routes_df, airline_meta = self.select_airline_routes(airline_query)
        processed = self.process_routes(routes_df)
        cost_df = self.cost_analysis(processed)
        median_distance = float(pd.to_numeric(cost_df.get("Distance (miles)"), errors="coerce").median()) if not cost_df.empty else None
        median_asm = float(pd.to_numeric(cost_df.get("ASM"), errors="coerce").median()) if not cost_df.empty else None
        existing = ((routes_df["Source airport"] == source_airport) & (routes_df["Destination airport"] == dest_airport)).any()

        # Hub alignment
        hub_fit_score = 0.5
        hub_fit_label = "Off-hub"
        top_hubs: list = []
        try:
            network = self.build_network(processed)
            network_stats = self.analyze_network(network)
            raw_hubs = network_stats.get("Top 5 Hubs") or []
            top_hubs = [entry[0] for entry in raw_hubs if isinstance(entry, (list, tuple)) and len(entry) >= 1]
            hits = sum(1 for airport in (source_airport, dest_airport) if airport in top_hubs)
            if hits == 2:
                hub_fit_score = 1.0
                hub_fit_label = "Hub-to-hub"
            elif hits == 1:
                hub_fit_score = 0.7
                hub_fit_label = "Hub-to-spoke"
            else:
                hub_fit_score = 0.35
                hub_fit_label = "Off-hub"
        except Exception:
            # Keep defaults if network build fails
            pass

        # Distance for proposed route
        distance = geodesic((src_row["Latitude"], src_row["Longitude"]), (dst_row["Latitude"], dst_row["Longitude"])).miles

        # Competition and market depth
        totals_lookup = self.get_route_total_asm([(source_airport, dest_airport)])
        market_asm = float(totals_lookup.get((source_airport, dest_airport)) or 0.0)
        competition_subset = self.routes[
            (self.routes["Source airport"] == source_airport)
            & (self.routes["Destination airport"] == dest_airport)
        ]
        competition_count = int(competition_subset["Airline Code"].nunique()) if not competition_subset.empty else 0

        def _competition_score(count):
            if count <= 1:
                return 1.0, "Monopoly"
            if count == 2:
                return 0.55, "Duopoly"
            if count <= 4:
                return 0.35, "Oligopoly"
            return 0.2, "Multi-Carrier"

        competition_score, competition_label = _competition_score(competition_count)

        def _distance_fit(value, reference):
            if not reference or reference <= 0:
                return 0.5
            return float(max(0.0, min(1.0, 1 - abs(value - reference) / reference)))

        distance_fit = _distance_fit(distance, median_distance)

        def _market_depth_score(value, reference):
            if not reference or reference <= 0:
                return 0.5
            scaled = value / (reference * 1.5)
            return float(max(0.0, min(1.0, scaled)))

        market_depth_score = _market_depth_score(market_asm, median_asm)

        # Analog demand: use airline's existing routes within ±20% distance
        analog_summary = {}
        if not cost_df.empty:
            distances = pd.to_numeric(cost_df.get("Distance (miles)"), errors="coerce")
            asm_series = pd.to_numeric(cost_df.get("ASM"), errors="coerce")
            competition_series = pd.to_numeric(cost_df.get("Competition Score"), errors="coerce")
            window_low = distance * 0.8
            window_high = distance * 1.2
            analogs = cost_df[(distances >= window_low) & (distances <= window_high)].copy()
            if analogs.empty:
                analogs = cost_df.copy()
            analogs = analogs.assign(
                _distance_gap=(pd.to_numeric(analogs["Distance (miles)"], errors="coerce") - distance).abs()
            ).sort_values(["_distance_gap", "ASM"], ascending=[True, False])
            top_analogs = analogs.head(5)
            analog_median_asm = float(pd.to_numeric(top_analogs.get("ASM"), errors="coerce").median() or 0.0)
            analog_median_competition = float(pd.to_numeric(top_analogs.get("Competition Score"), errors="coerce").median() or 0.0)
            analog_median_distance = float(pd.to_numeric(top_analogs.get("Distance (miles)"), errors="coerce").median() or 0.0)
            analog_seat_estimate = 0.0
            if analog_median_distance > 0:
                analog_seat_estimate = analog_median_asm / analog_median_distance
            sample_pairs = top_analogs[["Source airport", "Destination airport"]].head(3).apply(
                lambda row: f"{row['Source airport']}-{row['Destination airport']}", axis=1
            ).tolist()
            analog_summary = {
                "median_asm": round(analog_median_asm, 2),
                "median_competition_score": round(analog_median_competition, 3),
                "median_distance": round(analog_median_distance, 2),
                "estimated_seats": round(analog_seat_estimate, 2),
                "sample_routes": sample_pairs,
            }

        # Load factor target from operational benchmarks (resources/airline_operational_metrics.json)
        airline_normalized = airline_meta.get("Airline (Normalized)") if isinstance(airline_meta, dict) else normalize_name(airline_query)
        lf_target = LOAD_FACTOR_BENCHMARKS.get((airline_normalized or "").lower(), 0.85)

        score = 0.45 * competition_score + 0.35 * distance_fit + 0.2 * market_depth_score
        if score >= 0.65:
            recommendation = "good"
        elif score >= 0.45:
            recommendation = "watch"
        else:
            recommendation = "avoid"

        rationale = [
            f"Competition: {competition_label} ({competition_count} carriers)",
            f"Market depth vs. airline median ASM: {market_depth_score:.2f}",
            f"Distance fit vs. airline median: {distance_fit:.2f}",
        ]
        if existing:
            rationale.append("Airline already serves this route.")
        if seat_demand:
            rationale.append(f"Seat demand provided: {seat_demand}")

        # Dynamic playbook generation using computed metrics
        def _fmt_percent(value):
            try:
                return f"{value * 100:.0f}%"
            except Exception:
                return "-"

        playbook = [
            f"Hub fit: {hub_fit_label} (top hubs: {', '.join(top_hubs[:3]) if top_hubs else 'N/A'})",
            f"Competition: {competition_label} ({competition_count} carriers); score {competition_score:.2f}",
            f"Distance fit: {distance_fit:.2f} vs. airline median {median_distance:.0f} mi" if median_distance else f"Distance fit: {distance_fit:.2f}",
            f"Market depth proxy: ASM {int(market_asm or 0)} vs. airline median {int(median_asm or 0)}; score {market_depth_score:.2f}",
            f"Analog demand: median ASM {int(analog_summary.get('median_asm', 0))} with competition score {analog_summary.get('median_competition_score', 0):.2f}; sample routes {', '.join(analog_summary.get('sample_routes', [])) or 'N/A'}",
            f"Load factor target: >= {_fmt_percent(lf_target)} based on recent operational metrics",
        ]
        if seat_demand:
            playbook.append(f"Provided seat demand: {seat_demand} (compare to analog estimated seats {int(analog_summary.get('estimated_seats') or 0)})")

        return {
            "airline": airline_meta.get("Airline") if isinstance(airline_meta, dict) else airline_query,
            "airline_normalized": airline_meta.get("Airline (Normalized)") if isinstance(airline_meta, dict) else normalize_name(airline_query),
            "source": source_airport,
            "destination": dest_airport,
            "distance_miles": round(distance, 2),
            "market_asm": market_asm,
            "competition_count": competition_count,
            "competition_label": competition_label,
            "competition_score": round(competition_score, 3),
            "distance_fit": round(distance_fit, 3),
            "market_depth_score": round(market_depth_score, 3),
            "hub_fit_score": round(hub_fit_score, 3),
            "hub_fit_label": hub_fit_label,
            "top_hubs": top_hubs,
            "analog_summary": analog_summary,
            "load_factor_target": lf_target,
            "score": round(score, 3),
            "recommendation": recommendation,
            "already_served": bool(existing),
            "rationale": rationale,
            "playbook": playbook,
        }


    @staticmethod
    def _format_cbsa_cache_key(lat, lon):
        try:
            return f"{float(lat):.5f},{float(lon):.5f}"
        except Exception:
            return None

    @staticmethod
    def _extract_cbsa_city_tokens(cbsa_name):
        """Heuristic to derive constituent city names from a CBSA title."""
        if not isinstance(cbsa_name, str):
            return []
        city_block = cbsa_name.split(",")[0]
        return [token.strip() for token in city_block.split("-") if token.strip()]

    @staticmethod
    def _split_equipment_tokens(equipment_value):
        """Split equipment strings like 'ERD ER4 CRJ' into canonical tokens."""
        if not isinstance(equipment_value, str):
            return []
        cleaned = equipment_value.replace("/", " ")
        tokens = [
            token.strip().upper()
            for token in cleaned.split()
            if token and token.strip()
        ]
        return tokens

    @staticmethod
    def _normalize_equipment_code(code):
        if not isinstance(code, str):
            return None
        cleaned = "".join(ch for ch in code.upper() if ch.isalnum())
        return cleaned or None

    def _build_equipment_capacity_lookup(self, aircraft_df):
        """Create a lookup of equipment tokens to typical seat counts."""
        lookup = {}
        if aircraft_df is not None and not aircraft_df.empty:
            for _, row in aircraft_df.iterrows():
                seats = row.get("Total")
                if pd.isna(seats):
                    continue
                raw_code = str(row.get("Aircraft") or "").strip().upper()
                if not raw_code:
                    continue
                seat_value = int(seats)
                if raw_code not in lookup:
                    lookup[raw_code] = {"seats": seat_value, "source": "airline_config"}
                normalized = self._normalize_equipment_code(raw_code)
                if normalized:
                    if normalized not in lookup:
                        lookup[normalized] = {"seats": seat_value, "source": "airline_config"}

        for code, seats in GENERIC_SEAT_GUESSES.items():
            normalized = self._normalize_equipment_code(code)
            if normalized:
                lookup.setdefault(normalized, {"seats": seats, "source": "generic_guess"})
            lookup.setdefault(code.upper(), {"seats": seats, "source": "generic_guess"})

        self._equipment_lookup_keys = sorted(set(lookup.keys()))
        return lookup

    def _estimate_seat_capacity(self, equipment_value):
        """Return a best-effort seat count for the provided equipment string."""
        if not self.equipment_capacity_lookup:
            return None

        tokens = self._split_equipment_tokens(equipment_value)
        for token in tokens:
            normalized = self._normalize_equipment_code(token)
            for candidate in filter(None, (token, normalized)):
                record = self.equipment_capacity_lookup.get(candidate)
                if record:
                    return record

        if tokens and self._equipment_lookup_keys:
            for token in tokens:
                normalized = self._normalize_equipment_code(token) or token
                match = process.extractOne(
                    normalized,
                    self._equipment_lookup_keys,
                    scorer=fuzz.WRatio,
                    score_cutoff=88
                )
                if match:
                    record = self.equipment_capacity_lookup.get(match[0])
                    if record:
                        return record

        return None


    def lookup_cbsa_title(self, county, state):
        """Return CBSA title matching a county/state pair if present in local data."""
        if self.cbsa_lookup is None:
            return None
        if not isinstance(county, str) or not isinstance(state, str):
            return None
        county_key = county.strip().lower()
        state_key = state.strip().lower()
        matches = self.cbsa_lookup[
            (self.cbsa_lookup["county_key"] == county_key) &
            (self.cbsa_lookup["state_key"] == state_key)
        ]
        if matches.empty:
            return None
        # Multiple counties may map to the same CBSA; return the first match
        return matches.iloc[0]["CBSA Title"]


    def get_cbsa_metadata(self, lat, lon, country=None):
        """Fetch CBSA metadata for a given coordinate pair, using the FCC API with caching."""
        default_payload = {
            "County/County Equivalent": None,
            "State Name": None,
            "CBSA Name": "unknown",
            "CBSA Code": None
        }

        if country and isinstance(country, str) and country.strip().lower() not in {"united states", "united states minor outlying islands"}:
            # FCC CBSA coverage is US-only; skip lookups for other countries
            non_us_payload = default_payload.copy()
            non_us_payload["CBSA Name"] = "non-US"
            return non_us_payload

        cache_key = self._format_cbsa_cache_key(lat, lon)
        if cache_key is None:
            return default_payload

        cached = self.cbsa_cache.get(cache_key)
        if isinstance(cached, dict):
            return cached

        url = f"https://geo.fcc.gov/api/census/block/find?latitude={lat}&longitude={lon}&format=json"
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                county_info = data.get("County") or {}
                state_info = data.get("State") or {}
                cbsa_info = data.get("CBSA") or {}

                county_name = county_info.get("name")
                state_name = state_info.get("name")
                cbsa_name = cbsa_info.get("name")
                cbsa_code = cbsa_info.get("FIPS") or cbsa_info.get("code")

                if not cbsa_name and county_name and state_name:
                    cbsa_name = self.lookup_cbsa_title(county_name, state_name)

                payload = {
                    "County/County Equivalent": county_name,
                    "State Name": state_name,
                    "CBSA Name": cbsa_name or "unknown",
                    "CBSA Code": cbsa_code
                }
            else:
                payload = default_payload
        except (requests.RequestException, ValueError, json.JSONDecodeError):
            payload = default_payload

        self.cbsa_cache[cache_key] = payload
        self._cbsa_cache_dirty = True
        return payload


    def annotate_airports_with_cbsa(self, airports_df):
        """Attach CBSA metadata to a dataframe of airports, respecting the cache."""
        if airports_df.empty:
            return airports_df.copy()

        enriched_rows = []
        for _, row in airports_df.iterrows():
            metadata = self.get_cbsa_metadata(
                row.get("Latitude"),
                row.get("Longitude"),
                row.get("Country")
            )
            enriched_rows.append(metadata)

        metadata_df = pd.DataFrame(enriched_rows)
        combined = pd.concat([airports_df.reset_index(drop=True), metadata_df], axis=1)

        # Persist cache updates once per batch to avoid repeated disk writes
        self._save_cbsa_cache()

        return combined

    def build_cbsa_cache(self, countries=None, limit=None, chunk_size=200):
        """
        Prime the CBSA cache by annotating airports for the requested countries.
        Returns the number of airports processed.
        """
        if self.airports is None:
            raise ValueError("Airports must be loaded before building the CBSA cache.")

        candidate_airports = self.airports.copy()

        if countries:
            normalized = {
                str(country).strip().lower()
                for country in countries
                if isinstance(country, str) and country.strip()
            }
            if normalized:
                country_series = candidate_airports["Country"].astype(str).str.strip().str.lower()
                candidate_airports = candidate_airports[country_series.isin(normalized)]

        if candidate_airports.empty:
            return 0

        candidate_airports = candidate_airports.copy()
        candidate_airports["__cbsa_cache_key"] = candidate_airports.apply(
            lambda row: self._format_cbsa_cache_key(row.get("Latitude"), row.get("Longitude")),
            axis=1
        )
        candidate_airports = candidate_airports[candidate_airports["__cbsa_cache_key"].notna()]
        candidate_airports = candidate_airports[
            ~candidate_airports["__cbsa_cache_key"].isin(self.cbsa_cache)
        ]
        candidate_airports = candidate_airports.drop(columns="__cbsa_cache_key")

        if candidate_airports.empty:
            return 0

        if isinstance(limit, int) and limit > 0:
            candidate_airports = candidate_airports.head(limit)

        if chunk_size is None or chunk_size <= 0:
            chunk_size = 200
        chunk_size = int(chunk_size)

        total = len(candidate_airports)
        processed = 0

        for start in range(0, total, chunk_size):
            chunk = candidate_airports.iloc[start:start + chunk_size]
            self.annotate_airports_with_cbsa(chunk)
            processed += len(chunk)
            print(f"CBSA cache progress: {processed}/{total} airports", flush=True)

        return processed


    def get_airports_metadata(self):
        """Return airports enriched with CBSA details, building once on demand."""
        if self.airports_metadata is None and self.airports is not None:
            self.airports_metadata = self.annotate_airports_with_cbsa(self.airports)
        return self.airports_metadata


    def user_airline(self):
        """Prompt user for an airline and display the number of routes for that airline."""
        input_airline = input("Enter an airline: ")
        routes_df, _ = self.select_airline_routes(input_airline, verbose=True)
        return routes_df

    def select_airline_routes(self, airline_query, verbose=False):
        """Return the route dataframe for the airline that best matches the supplied query."""
        if not isinstance(airline_query, str) or not airline_query.strip():
            raise ValueError("Airline query must be a non-empty string.")

        norm_airline = normalize_name(airline_query)
        matched_airlines = process.extract(
            norm_airline,
            self.airlines["Airline (Normalized)"],
            scorer=fuzz.WRatio,
            limit=20
        )

        best_score = -1
        best_match = None
        highest_num_routes = -1

        for name, score, index in matched_airlines:
            row = self.airlines[self.airlines["Airline (Normalized)"] == name].iloc[0]
            Airline_IATA = row['IATA']
            airline_routes = self.calculate_total_airline_routes(Airline_IATA)
            row = row.copy()
            row["Total Routes"] = airline_routes

            if pd.notna(row['IATA']):
                if score > best_score:
                    best_score = score
                    highest_num_routes = airline_routes
                    best_match = row
                elif score == best_score and airline_routes > highest_num_routes:
                    highest_num_routes = airline_routes
                    best_match = row

        if best_match is None:
            raise ValueError(f"No airline match found for query '{airline_query}'.")

        matched_airline_IATA = best_match['IATA']
        matched_airline_name = best_match['Airline']
        matched_airline_name_normalized = best_match['Airline (Normalized)']

        if verbose:
            print(f"Best match: {matched_airline_name} with IATA code {matched_airline_IATA} and {best_match['Total Routes']} routes.")

        filtered_routes = self.routes[self.routes['Airline Code'].str.lower() == matched_airline_IATA.lower()].copy()
        filtered_routes['Airline'] = matched_airline_name
        filtered_routes['Airline (Normalized)'] = matched_airline_name_normalized
        filtered_routes = self._apply_codeshare_overrides(filtered_routes, matched_airline_name_normalized)
        return filtered_routes, best_match
    
    
    
    def calculate_total_airline_routes(self, airline_code):
        if not isinstance(airline_code, str) or airline_code.strip() == "":
            return 0
        
        filtered = self.routes[self.routes['Airline Code'].str.lower() == airline_code.lower()]
        return len(filtered)
    

    
    def build_network(self,routes_df):
        """Build a network graph from the routes DataFrame."""
        G = nx.DiGraph()

        for _, row in routes_df.iterrows():
            source = row['Source airport']
            destination = row['Destination airport']
            G.add_edge(source, destination)

        return G
    
    def analyze_network(self, G):
        """Analyze the network graph and return basic statistics."""
        
        num_airports = G.number_of_nodes()
        num_routes = G.number_of_edges()

        top_hubs = sorted(G.degree, key=lambda x: x[1], reverse=True)[:5]

        ## avg_degree = sum(dict(G.degree()).values()) / num_nodes if num_nodes > 0 else 0
        return {
            "Number of Aiports Flown To": num_airports,
            "Number of Routes Flown": num_routes,
            "Top 5 Hubs": top_hubs,
        }      

    def draw_network(self, G, layout='spring'):
        """Draw the network graph."""
        import matplotlib.pyplot as plt

        if layout == 'circular':
            pos = nx.circular_layout(G)
        elif layout == 'kamada':
            pos = nx.kamada_kawai_layout(G)
        else:
            pos = nx.spring_layout(G)
      
        plt.figure(figsize=(12, 8))
        nx.draw(G, pos, with_labels=True, node_size=50, font_size=8)
        plt.title("Airline Route Network")
        plt.show()  



    def process_routes(self, routes_df):
        """Enrich routes_df with airport coordinates and compute geodesic distances directly."""
        if (not self.equipment_capacity_lookup) and self.aircraft_config is not None and not self.aircraft_config.empty:
            self.equipment_capacity_lookup = self._build_equipment_capacity_lookup(self.aircraft_config)

        # Merge source airport info
        source_airports = self.airports[["IATA", "Name", "Latitude", "Longitude"]].rename(
            columns={
                "IATA": "Source airport",
                "Name": "Source Name",
                "Latitude": "Source Latitude",
                "Longitude": "Source Longitude"
            }
        )

        # Merge destination airport info
        dest_airports = self.airports[["IATA", "Name", "Latitude", "Longitude"]].rename(
            columns={
                "IATA": "Destination airport",
                "Name": "Destination Name",
                "Latitude": "Destination Latitude",
                "Longitude": "Destination Longitude"
            }
        )

        if self.aircraft_config is not None:
            seats_on_route = self.aircraft_config.copy()
        else:
            seats_on_route = pd.DataFrame(columns=["Airline", "Aircraft", "Total"])

        # Merge the routes_df with the aircraft configuration
        routes_df = routes_df.merge(
            seats_on_route,
            left_on=["Airline (Normalized)", "Equipment"],
            right_on=["Airline", "Aircraft"],
            how="left"
        )

        seat_metadata = routes_df["Equipment"].apply(self._estimate_seat_capacity)
        seat_estimates = seat_metadata.apply(
            lambda record: record.get("seats") if isinstance(record, dict) else None
        )
        seat_estimate_sources = seat_metadata.apply(
            lambda record: record.get("source") if isinstance(record, dict) else None
        )
        seat_estimates = pd.to_numeric(seat_estimates, errors="coerce")

        routes_df["Seat Source"] = "unknown"
        if "Total" in routes_df.columns:
            routes_df["Total"] = pd.to_numeric(routes_df["Total"], errors="coerce")
            config_mask = routes_df["Total"].notna()
            routes_df.loc[config_mask, "Seat Source"] = "airline_config"
        else:
            routes_df["Total"] = pd.NA

        estimate_mask = routes_df["Total"].isna() & seat_estimates.notna()
        routes_df.loc[estimate_mask, "Total"] = seat_estimates[estimate_mask]
        if not seat_estimate_sources.empty:
            mapped_sources = seat_estimate_sources.replace({"generic_guess": "equipment_estimate"})
            routes_df.loc[estimate_mask, "Seat Source"] = mapped_sources[estimate_mask].fillna("equipment_estimate")
        else:
            routes_df.loc[estimate_mask, "Seat Source"] = "equipment_estimate"

        routes_df["Total"] = routes_df["Total"].fillna(0).astype(float)
        routes_df["Seat Source"] = routes_df["Seat Source"].fillna("unknown")
        
        # Merge both into the routes_df
        enriched_df = routes_df.merge(source_airports, on="Source airport", how="left")
        enriched_df = enriched_df.merge(dest_airports, on="Destination airport", how="left")

        # Drop unnecessary columns
        if 'Airline_x' in enriched_df.columns and 'Airline_y' in enriched_df.columns:
            enriched_df = enriched_df.drop(columns=['Airline_y'])
            enriched_df = enriched_df.rename(columns={'Airline_x': 'Airline'})

      # Inline computation of distance (miles and kilometers)
        enriched_df["Distance (miles)"] = enriched_df.apply(
            lambda row: geodesic(
                (row["Source Latitude"], row["Source Longitude"]),
                (row["Destination Latitude"], row["Destination Longitude"])
            ).miles if pd.notna(row["Source Latitude"]) and pd.notna(row["Destination Latitude"]) else None,
            axis=1
        )

        enriched_df["Distance (km)"] = enriched_df["Distance (miles)"] * 1.60934

        # Temporarily set display options
        pd.set_option('display.max_rows', None)       # Show all rows
        pd.set_option('display.max_columns', None)    # Show all columns
        pd.set_option('display.width', None)          # Don’t wrap columns

        ## print(enriched_df[["Airline (Normalized)", "Source airport", "Destination airport", "Distance (miles)", "Equipment", "Total"]].head())
        return enriched_df
    
    def find_best_aircraft_for_route(self, airline_name, route_distance, seat_demand=None, top_n=3, cost_df=None):
        """
        Recommend the best-fitting aircraft type for a target stage length and seat demand.

        The ranking blends three signals:
        - Fleet utilization (how broadly the type is already used in the network)
        - Distance fit (how closely the type's typical stage length matches the request)
        - Seat fit (optional, only when seat_demand is provided)
        """
        if route_distance is None:
            raise ValueError("route_distance is required.")
        try:
            route_distance = float(route_distance)
        except (TypeError, ValueError):
            raise ValueError("route_distance must be numeric.")
        if route_distance <= 0:
            raise ValueError("route_distance must be positive.")

        normalized_name = normalize_name(airline_name) if airline_name else None
        airline_label = airline_name
        computed_cost_df = None
        if cost_df is None:
            if self.routes is None or self.airports is None:
                raise ValueError("Routes and airports must be loaded before analyzing aircraft choices.")
            routes_df, airline_meta = self.select_airline_routes(airline_name)
            airline_label = airline_meta.get("Airline", airline_label)
            normalized_name = airline_meta.get("Airline (Normalized)", normalized_name)
            processed = self.process_routes(routes_df)
            computed_cost_df = self.cost_analysis(processed)
        else:
            computed_cost_df = cost_df.copy()
            if normalized_name is None and "Airline (Normalized)" in computed_cost_df.columns:
                non_null = computed_cost_df["Airline (Normalized)"].dropna()
                if not non_null.empty:
                    normalized_name = non_null.mode().iloc[0]

        fleet_summary = self.summarize_fleet_utilization(computed_cost_df)
        if fleet_summary.empty:
            return pd.DataFrame(
                columns=[
                    "Airline",
                    "Equipment",
                    "Route Count",
                    "Average Distance",
                    "Total Distance",
                    "Utilization Score",
                    "Seat Capacity",
                    "Distance Fit",
                    "Seat Fit",
                    "Airline Load Factor",
                    "Load Factor Pressure",
                    "Optimal Score",
                ]
            )

        if not self.equipment_capacity_lookup and self.aircraft_config is not None and not self.aircraft_config.empty:
            self.equipment_capacity_lookup = self._build_equipment_capacity_lookup(self.aircraft_config)

        seat_lookup = {}
        if normalized_name and self.aircraft_config is not None and not self.aircraft_config.empty:
            seat_rows = self.aircraft_config[self.aircraft_config["Airline"] == normalized_name]
            if not seat_rows.empty:
                seat_lookup = seat_rows.set_index("Aircraft")["Total"].to_dict()

        def _seat_capacity(equipment):
            seats = seat_lookup.get(equipment)
            if seats:
                return int(seats)
            record = self._estimate_seat_capacity(equipment)
            if isinstance(record, dict):
                value = record.get("seats")
                if value:
                    return int(value)
            return 0

        def _distance_fit(avg_distance):
            avg = float(avg_distance) if pd.notna(avg_distance) else 0.0
            if avg <= 0 or route_distance <= 0:
                return 0.0
            return max(0.0, 1 - abs(avg - route_distance) / (avg + route_distance))

        route_seat_demand = float(seat_demand) if seat_demand not in (None, "") else None
        if route_seat_demand is not None and route_seat_demand <= 0:
            route_seat_demand = None

        def _seat_fit(seat_count):
            if route_seat_demand is None:
                return 1.0
            seats = float(seat_count) if pd.notna(seat_count) else 0.0
            if seats <= 0:
                return 0.0
            return max(0.0, 1 - abs(seats - route_seat_demand) / (seats + route_seat_demand))

        summary = fleet_summary.copy()
        summary["Airline"] = airline_label
        summary["Seat Capacity"] = summary["Equipment"].apply(_seat_capacity)
        summary["Distance Fit"] = summary["Average Distance"].apply(_distance_fit)
        summary["Seat Fit"] = summary["Seat Capacity"].apply(_seat_fit)

        load_factor_meta = self.operational_metrics.get(normalized_name or "", {})
        load_factor_value = load_factor_meta.get("load_factor") if isinstance(load_factor_meta, dict) else None
        load_factor_pressure = self._normalize_load_factor(load_factor_value) if load_factor_value is not None else None
        load_factor_multiplier = 1.0
        if load_factor_pressure is not None:
            load_factor_multiplier = round(0.9 + (0.2 * load_factor_pressure), 4)
            summary["Airline Load Factor"] = load_factor_value
            summary["Load Factor Pressure"] = load_factor_pressure
        else:
            summary["Airline Load Factor"] = pd.NA
            summary["Load Factor Pressure"] = pd.NA

        if route_seat_demand is None:
            utilization_weight = 0.65
            distance_weight = 0.35
            seat_weight = 0.0
        else:
            utilization_weight = 0.45
            distance_weight = 0.35
            seat_weight = 0.20

        if load_factor_pressure is not None and route_seat_demand is not None:
            seat_weight += 0.10 * load_factor_pressure
            total_weight = utilization_weight + distance_weight + seat_weight
            if total_weight > 0:
                utilization_weight /= total_weight
                distance_weight /= total_weight
                seat_weight /= total_weight

        base_score = (
            utilization_weight * summary["Utilization Score"] +
            distance_weight * summary["Distance Fit"] +
            seat_weight * summary["Seat Fit"]
        )
        if route_seat_demand is None:
            summary["Optimal Score"] = (base_score * summary["Distance Fit"] * load_factor_multiplier).round(3)
        else:
            summary["Optimal Score"] = (base_score * summary["Distance Fit"] * summary["Seat Fit"] * load_factor_multiplier).round(3)

        columns = [
            "Airline",
            "Equipment",
            "Route Count",
            "Average Distance",
            "Total Distance",
            "Utilization Score",
            "Seat Capacity",
            "Distance Fit",
            "Seat Fit",
            "Airline Load Factor",
            "Load Factor Pressure",
            "Optimal Score",
        ]
        summary = summary[columns]
        return summary.sort_values("Optimal Score", ascending=False).head(int(top_n)).reset_index(drop=True)

    def _calculate_route_strategy_baseline(self, cost_df):
        """
        Build a baseline score for each route using multiple signals.

        The score blends:
        - An ASM share component that compares the airline's ASM to total ASM for the same
          route as observed in the global routes database.
        - A seat-density component that rewards routes operating above the airline's typical
          seats-per-mile performance.
        - A distance-alignment component that measures how closely the route aligns with the
          carrier's median stage length.
        """
        if cost_df is None or cost_df.empty:
            return pd.Series(dtype="float64")

        df = cost_df.copy()
        df_index = df.index
        asm_values = pd.to_numeric(df.get("ASM"), errors="coerce").fillna(0.0)
        spm_values = pd.to_numeric(df.get("Seats per Mile"), errors="coerce").fillna(0.0)
        distance_values = pd.to_numeric(df.get("Distance (miles)"), errors="coerce").fillna(0.0)

        route_pairs = list(zip(df.get("Source airport"), df.get("Destination airport")))
        totals_lookup = self.get_route_total_asm(route_pairs)
        totals_series = pd.Series(
            [
                totals_lookup.get(pair) if isinstance(pair, tuple) else None
                for pair in route_pairs
            ],
            index=df_index,
            dtype="float64"
        )
        totals_series = pd.to_numeric(totals_series, errors="coerce")

        asm_share = (asm_values / totals_series)
        asm_share = asm_share.where(totals_series > 0).clip(lower=0.0, upper=1.0).fillna(0.0)

        spm_reference = spm_values[spm_values > 0].median()
        if pd.isna(spm_reference) or spm_reference <= 0:
            seat_density = pd.Series(0.0, index=df_index, dtype="float64")
        else:
            seat_density = (spm_values / (spm_reference * 1.5)).clip(lower=0.0, upper=1.0)

        distance_reference = distance_values[distance_values > 0].median()
        if pd.isna(distance_reference) or distance_reference <= 0:
            distance_alignment = pd.Series(0.5, index=df_index, dtype="float64")
        else:
            distance_alignment = 1 - (distance_values - distance_reference).abs() / distance_reference
            distance_alignment = distance_alignment.clip(lower=0.0, upper=1.0)

        baseline = (
            0.5 * asm_share +
            0.3 * seat_density +
            0.2 * distance_alignment
        ).clip(lower=0.0, upper=1.0)

        return baseline.round(4)

    def _compute_route_competition(self, cost_df):
        """
        Annotate each route with an estimated competition level using the full routes dataset.
        Monopoly: only one airline flies the O&D, Duopoly: two airlines, Oligopoly: 3-4 airlines, Multi-Carrier: 5+ airlines.
        """
        if cost_df is None or cost_df.empty:
            return pd.Series(dtype="float64"), pd.Series(dtype=object), pd.Series(dtype="float64")

        if self.routes is not None and not self.routes.empty:
            competition_counts = (
                self.routes.groupby(["Source airport", "Destination airport"])["Airline Code"]
                .nunique()
                .to_dict()
            )
        else:
            competition_counts = {}

        counts = []
        levels = []
        scores = []

        def _level_from_count(value):
            if value <= 1:
                return "Monopoly", 1.0
            if value == 2:
                return "Duopoly", 0.55
            if value <= 4:
                return "Oligopoly", 0.35
            return "Multi-Carrier", 0.2

        for _, row in cost_df.iterrows():
            key = (row.get("Source airport"), row.get("Destination airport"))
            count = competition_counts.get(key, 1)
            level, score = _level_from_count(count)
            counts.append(float(count))
            levels.append(level)
            scores.append(score)

        return (
            pd.Series(counts, index=cost_df.index, dtype="float64"),
            pd.Series(levels, index=cost_df.index, dtype=object),
            pd.Series(scores, index=cost_df.index, dtype="float64"),
        )

    def _compute_route_maturity_proxy(self, cost_df):
        """
        Approximate route maturity using percentile bands of ASM within the airline network.
        Higher percentile => more established; lower percentile => emerging.
        """
        if cost_df is None or cost_df.empty:
            return pd.Series(dtype="float64"), pd.Series(dtype=object)

        asm_values = pd.to_numeric(cost_df.get("ASM"), errors="coerce").fillna(0.0)
        positive_mask = asm_values > 0
        if not positive_mask.any():
            return (
                pd.Series(0.5, index=cost_df.index, dtype="float64"),
                pd.Series("Unknown", index=cost_df.index, dtype=object),
            )

        percentile = asm_values.rank(pct=True, method="average")
        maturity_score = percentile.clip(lower=0.0, upper=1.0)

        def _label(value):
            if value >= 0.75:
                return "Established"
            if value >= 0.4:
                return "Maturing"
            return "Emerging"

        labels = maturity_score.apply(_label)
        return maturity_score.round(4), labels

    def _compute_route_yield_proxy(self, cost_df):
        """
        Normalize seats-per-mile via percentile ranks to approximate pricing pressure.
        Lower seat density (low SPM percentile) -> higher yield score.
        """
        if cost_df is None or cost_df.empty:
            return pd.Series(dtype="float64")

        spm_values = pd.to_numeric(cost_df.get("Seats per Mile"), errors="coerce").fillna(0.0)
        positive_mask = spm_values > 0
        if not positive_mask.any():
            return pd.Series(0.5, index=cost_df.index, dtype="float64")

        percentile = spm_values.rank(pct=True, method="average")
        yield_score = (1 - percentile).clip(lower=0.0, upper=1.0)
        return yield_score.round(4)

    def build_route_scorecard(self, cost_df):
        """Create a summary of competition, maturity, and yield proxies for dashboards."""
        if cost_df is None or cost_df.empty:
            return {
                "competition": {},
                "maturity": {},
                "yield": {},
            }

        df = cost_df.copy()
        asm = pd.to_numeric(df.get("ASM"), errors="coerce").fillna(0.0)
        total_asm = asm.sum() or 1.0

        comp_profile = (
            df.groupby("Competition Level")["ASM"]
            .sum(min_count=1)
            .div(total_asm)
            .fillna(0.0)
            .to_dict()
        )
        maturity_profile = (
            df.groupby("Route Maturity Label")["ASM"]
            .sum(min_count=1)
            .div(total_asm)
            .fillna(0.0)
            .to_dict()
        )
        yield_percentiles = {
            "p25": float(df["Yield Proxy Score"].quantile(0.25)) if "Yield Proxy Score" in df else None,
            "p50": float(df["Yield Proxy Score"].median()) if "Yield Proxy Score" in df else None,
            "p75": float(df["Yield Proxy Score"].quantile(0.75)) if "Yield Proxy Score" in df else None,
        }

        return {
            "competition": comp_profile,
            "maturity": maturity_profile,
            "yield": yield_percentiles,
        }

    def compute_market_share_snapshot(self, cost_df, limit=20, include_all_competitors=True):
        """
        Produce a weighted market-share snapshot for the airline's largest O&D routes.
        Uses the global ASM total for each airport pair as the denominator unless constrained.
        """
        if cost_df is None or cost_df.empty:
            return pd.DataFrame(columns=[
                "Source",
                "Destination",
                "Airline ASM",
                "Market ASM",
                "Market Share",
                "Competition Level",
            ])

        df = cost_df.copy()
        df = df.sort_values("ASM", ascending=False).head(limit).reset_index(drop=True)
        route_pairs = list(zip(df["Source airport"], df["Destination airport"]))
        totals = self.get_route_total_asm(route_pairs) if include_all_competitors else {}
        if include_all_competitors:
            df["Market ASM"] = [
                totals.get(pair, row["ASM"]) if isinstance(pair, tuple) else row["ASM"]
                for pair, row in zip(route_pairs, df.to_dict(orient="records"))
            ]
        else:
            df["Market ASM"] = df["ASM"]
        df["Market Share"] = df.apply(
            lambda row: (row["ASM"] / row["Market ASM"]) if row["Market ASM"] and row["Market ASM"] > 0 else None,
            axis=1
        )
        snapshot = df[[
            "Source airport",
            "Destination airport",
            "ASM",
            "Market ASM",
            "Market Share",
            "Competition Level",
        ]].copy()
        snapshot.rename(
            columns={
                "Source airport": "Source",
                "Destination airport": "Destination",
                "ASM": "Airline ASM",
            },
            inplace=True,
        )
        return snapshot

    def analyze_route_market_share(self, route_pairs, top_airlines=5, include_all_competitors=True):
        """
        Analyze market share and competitive stats for the provided city pairs.
        Returns the top airlines per route ordered by ASM contribution.
        """
        if not route_pairs:
            raise ValueError("At least one route is required.")
        if any(dataset is None for dataset in (self.routes, self.airlines, self.airports, self.aircraft_config)):
            raise ValueError("Route, airline, airport, and equipment data must be loaded.")

        normalized_pairs = []
        seen = set()
        for pair in route_pairs:
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                continue
            source, destination = pair
            src_code = str(source or "").strip().upper()
            dest_code = str(destination or "").strip().upper()
            if not src_code or not dest_code:
                continue
            key = (src_code, dest_code)
            if key not in seen:
                seen.add(key)
                normalized_pairs.append(key)

        if not normalized_pairs:
            raise ValueError("No valid routes were provided.")

        top_limit = max(1, min(int(top_airlines or 1), 20))
        route_set = set(normalized_pairs)
        source_filter = {src for src, _ in normalized_pairs}
        dest_filter = {dest for _, dest in normalized_pairs}

        routes_df = self.routes.copy()
        routes_df["_source"] = routes_df["Source airport"].astype(str).str.upper()
        routes_df["_dest"] = routes_df["Destination airport"].astype(str).str.upper()
        mask = routes_df["_source"].isin(source_filter) & routes_df["_dest"].isin(dest_filter)
        subset = routes_df.loc[mask].copy()
        subset["Source airport"] = subset["_source"]
        subset["Destination airport"] = subset["_dest"]
        subset["__pair_key"] = list(zip(subset["Source airport"], subset["Destination airport"]))
        subset = subset[subset["__pair_key"].isin(route_set)].copy()
        subset.drop(columns=["_source", "_dest", "__pair_key"], inplace=True, errors="ignore")

        airline_columns = {"IATA", "Airline", "Airline (Normalized)"}
        if self.airlines is not None and airline_columns.issubset(set(self.airlines.columns)):
            lookup = self.airlines.loc[:, ["IATA", "Airline", "Airline (Normalized)"]]
            subset = subset.merge(lookup, left_on="Airline Code", right_on="IATA", how="left")
            subset["Airline"] = subset["Airline"].where(subset["Airline"].notna(), subset["Airline Code"])
            subset["Airline (Normalized)"] = subset["Airline (Normalized)"].where(
                subset["Airline (Normalized)"].notna(),
                subset["Airline"].astype(str).str.lower(),
            )
            subset.drop(columns=["IATA"], inplace=True, errors="ignore")
        else:
            subset["Airline"] = subset["Airline Code"]
            subset["Airline (Normalized)"] = subset["Airline Code"].astype(str).str.lower()

        processed = self.process_routes(subset) if not subset.empty else pd.DataFrame(columns=self.routes.columns)
        cost_df = self.cost_analysis(processed) if not processed.empty else pd.DataFrame()
        if not cost_df.empty and "ASM Valid" in cost_df.columns:
            valid = cost_df[cost_df["ASM Valid"]].copy()
        else:
            valid = pd.DataFrame()
        if not valid.empty:
            valid["Route Pair"] = list(zip(valid["Source airport"], valid["Destination airport"]))
            valid = valid[valid["Route Pair"].isin(route_set)].copy()

        totals_lookup = self.get_route_total_asm(normalized_pairs) if include_all_competitors else {}

        def _pick_mode(series: pd.Series):
            if series is None or series.empty:
                return None
            mode_values = series.mode(dropna=True)
            if not mode_values.empty:
                return mode_values.iloc[0]
            filtered = series.dropna()
            if filtered.empty:
                return None
            return filtered.iloc[0]

        def _mean(series: pd.Series):
            if series is None or series.empty:
                return None
            filtered = series.dropna()
            if filtered.empty:
                return None
            return float(filtered.mean())

        def _to_float(value):
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        route_payload = {}
        for pair in normalized_pairs:
            market_total = _to_float(totals_lookup.get(pair))
            route_payload[pair] = {
                "source": pair[0],
                "destination": pair[1],
                "distance_miles": None,
                "market_asm": market_total,
                "competitor_count": None,
                "competition_level": None,
                "route_maturity_label": None,
                "yield_proxy_score": None,
                "status": "not_found",
                "airlines": [],
            }

        if not valid.empty:
            for route_pair, route_group in valid.groupby("Route Pair"):
                summary = route_payload.get(route_pair)
                if summary is None:
                    continue
                summary["distance_miles"] = _mean(route_group["Distance (miles)"]) or summary["distance_miles"]
                competitor_counts = route_group.get("Competitor Count")
                if competitor_counts is not None:
                    competitor_counts = competitor_counts.dropna()
                    if not competitor_counts.empty:
                        summary["competitor_count"] = int(max(summary.get("competitor_count") or 0, competitor_counts.max()))
                if "Competition Level" in route_group:
                    summary["competition_level"] = summary["competition_level"] or _pick_mode(route_group["Competition Level"])
                if "Route Maturity Label" in route_group:
                    summary["route_maturity_label"] = summary["route_maturity_label"] or _pick_mode(
                        route_group["Route Maturity Label"]
                    )
                if "Yield Proxy Score" in route_group:
                    summary["yield_proxy_score"] = _mean(route_group["Yield Proxy Score"])

                if include_all_competitors:
                    if summary.get("market_asm") is None:
                        summary["market_asm"] = float(route_group["ASM"].sum()) if "ASM" in route_group else None

                airline_grouped = route_group.groupby("Airline", dropna=False)
                for airline_name, airline_slice in airline_grouped:
                    normalized_label = airline_slice["Airline (Normalized)"].iloc[0] if "Airline (Normalized)" in airline_slice else None
                    asm_value = float(airline_slice["ASM"].sum()) if "ASM" in airline_slice else 0.0
                    seats_total = None
                    if "Total Seats" in airline_slice and airline_slice["Total Seats"].notna().any():
                        seats_total = float(airline_slice["Total Seats"].sum())
                    seats_per_mile = _mean(airline_slice["Seats per Mile"]) if "Seats per Mile" in airline_slice else None
                    strategy_score = _mean(airline_slice["Route Strategy Baseline"]) if "Route Strategy Baseline" in airline_slice else None
                    yield_score = _mean(airline_slice["Yield Proxy Score"]) if "Yield Proxy Score" in airline_slice else None
                    maturity = (
                        _pick_mode(airline_slice["Route Maturity Label"])
                        if "Route Maturity Label" in airline_slice
                        else None
                    )
                    equipment_values = []
                    if "Equipment" in airline_slice:
                        equipment_values = sorted(
                            {
                                str(value).strip().upper()
                                for value in airline_slice["Equipment"].dropna()
                                if isinstance(value, str) and value.strip()
                            }
                        )
                    market_asm = summary.get("market_asm") or asm_value or None
                    share = None
                    if market_asm:
                        share = asm_value / market_asm if market_asm > 0 else None

                    summary["airlines"].append(
                        {
                            "airline": airline_name or normalized_label,
                            "airline_normalized": normalized_label,
                            "asm": asm_value,
                            "market_share": share,
                            "seats": seats_total,
                            "seats_per_mile": seats_per_mile,
                            "equipment": equipment_values,
                            "route_strategy_baseline": strategy_score,
                            "yield_proxy_score": yield_score,
                            "route_maturity_label": maturity,
                        }
                    )
                if summary["airlines"]:
                    summary["status"] = "ok"

        ordered_results = []
        for pair in normalized_pairs:
            summary = route_payload.get(pair)
            if summary is None:
                continue
            airlines = summary.get("airlines", [])
            airlines.sort(key=lambda entry: entry.get("asm") or 0.0, reverse=True)
            summary["airline_count"] = len(airlines)
            if not include_all_competitors:
                # When constrained, recompute denominator using only included airlines.
                market_total = sum(entry.get("asm") or 0.0 for entry in airlines) or None
                summary["market_asm"] = market_total
                for entry in airlines:
                    asm_value = entry.get("asm") or 0.0
                    entry["market_share"] = (asm_value / market_total) if market_total and market_total > 0 else None
            summary["airlines"] = airlines[:top_limit]
            if summary["status"] != "ok" and not summary["airlines"]:
                summary["status"] = "not_found"
            ordered_results.append(summary)

        return {"routes": ordered_results}

    def summarize_fleet_utilization(self, cost_df):
        """
        Approximate fleet utilization context using network coverage.
        Calculates route counts, average distance, and a utilization score per aircraft type.
        """
        if cost_df is None or cost_df.empty:
            return pd.DataFrame(
                columns=["Equipment", "Route Count", "Average Distance", "Total Distance", "Utilization Score"]
            )

        df = cost_df.copy()
        df["Equipment"] = df.get("Equipment").fillna("Unknown")
        df["Distance (miles)"] = pd.to_numeric(df.get("Distance (miles)"), errors="coerce").fillna(0.0)
        df["Total Seats"] = pd.to_numeric(df.get("Total Seats"), errors="coerce").fillna(0.0)
        if "ASM" in df.columns:
            asm = pd.to_numeric(df["ASM"], errors="coerce")
        else:
            asm = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
        fallback_asm = df["Total Seats"] * df["Distance (miles)"]
        df["ASM"] = asm.where(asm.notna(), fallback_asm)
        df["_route_key"] = df["Source airport"].astype(str) + "-" + df["Destination airport"].astype(str)

        def _equipment_tokens(value):
            tokens = self._split_equipment_tokens(value)
            return tokens if tokens else ["UNKNOWN"]

        df["_equipment_tokens"] = df["Equipment"].apply(_equipment_tokens)
        df["_token_count"] = df["_equipment_tokens"].apply(len).clip(lower=1)
        exploded = df.explode("_equipment_tokens").copy()
        exploded["Equipment"] = exploded["_equipment_tokens"].fillna("UNKNOWN")
        exploded["_asm_share"] = exploded["ASM"] / exploded["_token_count"]

        grouped = (
            exploded.groupby("Equipment", dropna=False)
            .agg(
                Route_Count=("_route_key", "nunique"),
                Average_Distance=("Distance (miles)", "mean"),
                Total_Distance=("Distance (miles)", "sum"),
                ASM_Total=("_asm_share", "sum"),
            )
            .reset_index()
        )
        asm_total = grouped["ASM_Total"].sum()
        grouped.rename(
            columns={
                "Route_Count": "Route Count",
                "Average_Distance": "Average Distance",
                "Total_Distance": "Total Distance",
            },
            inplace=True,
        )
        if asm_total <= 0:
            grouped["ASM_Total"] = grouped["Route Count"]
            asm_total = grouped["ASM_Total"].sum()

        route_weight_total = grouped["Route Count"].sum() or 1.0
        asm_total = asm_total or 1.0

        asm_share = grouped["ASM_Total"] / asm_total
        route_share = grouped["Route Count"] / route_weight_total
        grouped["Utilization Score"] = (0.6 * asm_share + 0.4 * route_share).clip(lower=0.0, upper=1.0)

        grouped["Average Distance"] = grouped["Average Distance"].round(2)
        grouped["Total Distance"] = grouped["Total Distance"].round(2)
        grouped["Utilization Score"] = grouped["Utilization Score"].round(3)
        grouped = grouped.drop(columns=["ASM_Total"]).sort_values(
            ["Utilization Score", "Route Count"], ascending=[False, False]
        )
        return grouped.reset_index(drop=True)

    def cost_analysis(self, airline_df, include_strategy_baseline=True):
        """Perform cost analysis on the airline DataFrame."""
        # Calculate total seats available for each route
        df = airline_df.copy()
        df["Total Seats"] = pd.to_numeric(df.get("Total"), errors="coerce").fillna(0.0)
        df["Distance (miles)"] = pd.to_numeric(df.get("Distance (miles)"), errors="coerce").fillna(0.0)

        """Calculate seats per mile for each route."""
        df["Seats per Mile"] = df.apply(
            lambda row: row["Total Seats"] / row["Distance (miles)"] if row["Distance (miles)"] > 0 else 0,
            axis=1
        )

        df["ASM"] = df["Total Seats"] * df["Distance (miles)"]
        df["ASM Valid"] = (df["Distance (miles)"] > 0) & (df["Total Seats"] > 0)
        if include_strategy_baseline:
            baseline_score = self._calculate_route_strategy_baseline(df)
            if baseline_score.empty:
                baseline_score = pd.Series(0.0, index=df.index, dtype="float64")
            df["Route Strategy Baseline"] = baseline_score.reindex(df.index).fillna(0.0)
        else:
            df["Route Strategy Baseline"] = 0.0

        competition_count, competition_level, competition_score = self._compute_route_competition(df)
        df["Competitor Count"] = competition_count.reindex(df.index).fillna(1.0)
        df["Competition Level"] = competition_level.reindex(df.index).fillna("Monopoly")
        df["Competition Score"] = competition_score.reindex(df.index).fillna(1.0)

        maturity_score, maturity_label = self._compute_route_maturity_proxy(df)
        df["Route Maturity Score"] = maturity_score.reindex(df.index).fillna(0.5)
        df["Route Maturity Label"] = maturity_label.reindex(df.index).fillna("Unknown")

        yield_proxy = self._compute_route_yield_proxy(df)
        df["Yield Proxy Score"] = yield_proxy.reindex(df.index).fillna(0.5)

        columns = [
            "Airline",
            "Airline (Normalized)",
            "Source airport",
            "Destination airport",
            "Distance (miles)",
            "Total Seats",
            "Seats per Mile",
            "ASM",
            "ASM Valid",
            "Equipment",
        ]
        if "Seat Source" in df.columns:
            columns.append("Seat Source")
        columns.append("Route Strategy Baseline")
        columns.extend(
            [
                "Competitor Count",
                "Competition Level",
                "Competition Score",
                "Route Maturity Score",
                "Route Maturity Label",
                "Yield Proxy Score",
            ]
        )

        ## print(airline_df[["Airline", "Source airport", "Destination airport", "Distance (miles)", "Total Seats", "Seats per Mile"]].head())
        return df[columns]

    def simulate_fleet_assignment(
        self,
        airline_query,
        fleet_config,
        route_limit=80,
        day_hours=18.0,
        maintenance_hours=6.0,
        crew_max_hours=14.0,
        taxi_buffer_minutes=20,
        default_turn_minutes=45,
    ):
        """
        Greedy fleet assignment simulator that approximates a daily schedule.

        Args:
            airline_query: Airline identifier to derive the representative route list.
            fleet_config: List of {"equipment": str, "count": int}.
            route_limit: Maximum number of routes (sorted by ASM) to include in the simulation.
            day_hours: Total length of the operating day considered by the simulator.
            maintenance_hours: Required daily maintenance window for each tail.
            crew_max_hours: Maximum flight (block) hours allowed per tail within the day.
            taxi_buffer_minutes: Fixed taxi/approach buffer per flight, applied to the block time.
            default_turn_minutes: Minimum turn time to assume when equipment-specific heuristics are unavailable.
        """
        if not isinstance(fleet_config, list) or not fleet_config:
            raise ValueError("At least one fleet entry is required.")

        if not isinstance(day_hours, (int, float)) or day_hours <= 0:
            raise ValueError("Operating day must be greater than zero hours.")

        if maintenance_hours < 0:
            raise ValueError("Maintenance hours cannot be negative.")

        if maintenance_hours >= day_hours:
            raise ValueError("Maintenance window must be shorter than the operating day.")

        max_operating_window = day_hours - maintenance_hours
        crew_limit = min(float(crew_max_hours or 0), max_operating_window)
        if crew_limit <= 0:
            raise ValueError("Crew hour limit leaves no time for flight assignment.")

        route_limit = max(1, min(int(route_limit or 1), 500))
        taxi_buffer_hours = max(0.0, float(taxi_buffer_minutes or 0) / 60.0)
        base_turn_hours = max(0.1, float(default_turn_minutes or 45) / 60.0)

        routes_df, airline_meta = self.select_airline_routes(airline_query)
        processed = self.process_routes(routes_df)
        cost_df = self.cost_analysis(processed)
        if cost_df.empty:
            raise ValueError("No routes available for fleet assignment.")

        flights_df = cost_df.sort_values("ASM", ascending=False).head(route_limit).copy()
        if flights_df.empty:
            raise ValueError("Unable to extract representative routes for simulation.")

        def _seat_capacity_for_equipment(value):
            record = self._estimate_seat_capacity(value)
            if isinstance(record, dict):
                seats = record.get("seats")
                if seats:
                    return int(seats)
            return None

        def _categorize_equipment(seat_count):
            seats = seat_count if seat_count and seat_count > 0 else 150
            if seats >= 260:
                return {"category": "widebody", "cruise_speed": 515.0, "turn_hours": 1.5, "range_miles": 6500}
            if seats >= 150:
                return {"category": "narrowbody", "cruise_speed": 500.0, "turn_hours": 1.0, "range_miles": 3500}
            if seats >= 90:
                return {"category": "crossover", "cruise_speed": 470.0, "turn_hours": 0.75, "range_miles": 2200}
            return {"category": "regional", "cruise_speed": 430.0, "turn_hours": 0.5, "range_miles": 1200}

        resolved_fleet = []
        for entry in fleet_config:
            equipment_name = (entry.get("equipment") or "").strip()
            count = int(entry.get("count") or 0)
            if not equipment_name or count <= 0:
                continue
            seat_guess = _seat_capacity_for_equipment(equipment_name)
            equipment_profile = _categorize_equipment(seat_guess)
            resolved_fleet.append(
                {
                    "equipment": equipment_name,
                    "count": count,
                    "seat_capacity": seat_guess if seat_guess and seat_guess > 0 else 150,
                    "category": equipment_profile["category"],
                    "cruise_speed": equipment_profile["cruise_speed"],
                    "turn_hours": max(equipment_profile["turn_hours"], base_turn_hours),
                    "range_miles": equipment_profile["range_miles"],
                }
            )

        if not resolved_fleet:
            raise ValueError("Fleet configuration did not include any valid entries.")

        tails = []
        for fleet_entry in resolved_fleet:
            for index in range(fleet_entry["count"]):
                tails.append(
                    {
                        "tail_id": f"{fleet_entry['equipment'].upper()}-{index + 1:02d}",
                        "equipment": fleet_entry["equipment"].upper(),
                        "category": fleet_entry["category"],
                        "seat_capacity": fleet_entry["seat_capacity"],
                        "cruise_speed": fleet_entry["cruise_speed"],
                        "turn_time": fleet_entry["turn_hours"],
                        "max_range": fleet_entry["range_miles"],
                        "next_available": 0.0,
                        "block_hours": 0.0,
                        "assignments": [],
                    }
                )

        if not tails:
            raise ValueError("Fleet configuration resolved to zero tails.")

        def _format_clock(hour_value, start_offset=5.0):
            total_hours = start_offset + hour_value
            total_minutes = int(round(total_hours * 60))
            hours_component = (total_minutes // 60) % 24
            minutes_component = total_minutes % 60
            return f"{hours_component:02d}:{minutes_component:02d}"

        flights = flights_df.to_dict(orient="records")
        assignments = []
        unassigned = []
        total_block_hours = 0.0

        for record in flights:
            required_seats = record.get("Total Seats")
            try:
                required_seats = float(required_seats)
            except (TypeError, ValueError):
                required_seats = None
            if not required_seats or required_seats <= 0:
                seat_record = _seat_capacity_for_equipment(record.get("Equipment"))
                required_seats = float(seat_record) if seat_record else 120.0
            required_seats = max(40.0, required_seats)

            distance_miles = record.get("Distance (miles)")
            try:
                distance_miles = float(distance_miles)
            except (TypeError, ValueError):
                distance_miles = None
            if not distance_miles or distance_miles <= 0:
                distance_miles = 500.0

            best_tail = None
            best_candidate_block = None
            best_score = None
            capacity_candidate = False
            schedule_candidate = False
            crew_candidate = False

            for tail in tails:
                if tail["max_range"] + 1e-6 < distance_miles:
                    continue
                capacity_candidate = True
                if tail["seat_capacity"] < required_seats * 0.75:
                    continue

                block_time = max(distance_miles / tail["cruise_speed"], 0.5) + taxi_buffer_hours
                tentative_end = tail["next_available"] + block_time
                next_available = tentative_end + tail["turn_time"]

                if next_available > max_operating_window + 1e-6:
                    continue
                schedule_candidate = True

                if tail["block_hours"] + block_time > crew_limit + 1e-6:
                    continue
                crew_candidate = True

                seat_gap = abs(tail["seat_capacity"] - required_seats)
                score = (
                    round(tail["next_available"], 3),
                    seat_gap,
                    round(tail["block_hours"], 3),
                )
                if best_score is None or score < best_score:
                    best_score = score
                    best_tail = tail
                    best_candidate_block = block_time

            if best_tail is None:
                if not capacity_candidate:
                    reason = "No fleet type can cover the route distance/seat demand."
                elif not schedule_candidate:
                    reason = "All compatible aircraft are outside the operating window."
                elif not crew_candidate:
                    reason = "Crew duty limits exhausted for compatible aircraft."
                else:
                    reason = "No available aircraft matched the request."
                unassigned.append(
                    {
                        "route": f"{record.get('Source airport', '?')} → {record.get('Destination airport', '?')}",
                        "equipment": record.get("Equipment"),
                        "distance_miles": round(distance_miles, 1),
                        "required_seats": round(required_seats, 1),
                        "reason": reason,
                    }
                )
                continue

            start_time = best_tail["next_available"]
            end_time = start_time + best_candidate_block
            best_tail["next_available"] = end_time + best_tail["turn_time"]
            best_tail["block_hours"] += best_candidate_block
            total_block_hours += best_candidate_block

            assignment_entry = {
                "route": f"{record.get('Source airport', '?')} → {record.get('Destination airport', '?')}",
                "source": record.get("Source airport"),
                "destination": record.get("Destination airport"),
                "tail_id": best_tail["tail_id"],
                "assigned_equipment": best_tail["equipment"],
                "equipment_requested": record.get("Equipment"),
                "block_hours": round(best_candidate_block, 2),
                "turn_hours": round(best_tail["turn_time"], 2),
                "start_hour": round(start_time, 2),
                "end_hour": round(end_time, 2),
                "start_label": _format_clock(start_time),
                "end_label": _format_clock(end_time),
                "distance_miles": round(distance_miles, 1),
                "required_seats": round(required_seats, 0),
            }
            assignments.append(assignment_entry)
            best_tail["assignments"].append(assignment_entry)

        total_flights = len(flights)
        scheduled_flights = len(assignments)
        available_block_hours = len(tails) * crew_limit
        utilization = (total_block_hours / available_block_hours) if available_block_hours > 0 else 0.0

        tail_logs = []
        for tail in tails:
            maintenance_buffer = max(day_hours - tail["next_available"], maintenance_hours)
            tail_logs.append(
                {
                    "tail_id": tail["tail_id"],
                    "equipment": tail["equipment"],
                    "category": tail["category"],
                    "flights": len(tail["assignments"]),
                    "block_hours": round(tail["block_hours"], 2),
                    "duty_hours": round(tail["next_available"], 2),
                    "utilization": round((tail["block_hours"] / crew_limit) if crew_limit > 0 else 0.0, 3),
                    "maintenance_buffer": round(maintenance_buffer, 2),
                }
            )

        airline_label = airline_meta.get("Airline") if isinstance(airline_meta, dict) else airline_query
        normalized_label = airline_meta.get("Airline (Normalized)") if isinstance(airline_meta, dict) else None

        fleet_overview = [
            {
                "equipment": entry["equipment"].upper(),
                "count": entry["count"],
                "seat_capacity": entry["seat_capacity"],
                "category": entry["category"],
            }
            for entry in resolved_fleet
        ]

        summary = {
            "airline": airline_label,
            "normalized": normalized_label,
            "total_flights": total_flights,
            "scheduled_flights": scheduled_flights,
            "coverage": round((scheduled_flights / total_flights) if total_flights else 0.0, 3),
            "total_block_hours": round(total_block_hours, 2),
            "available_block_hours": round(available_block_hours, 2),
            "utilization": round(utilization, 3),
            "unassigned": len(unassigned),
        }

        return {
            "airline": airline_label,
            "normalized": normalized_label,
            "parameters": {
                "day_hours": day_hours,
                "maintenance_hours": maintenance_hours,
                "crew_max_hours": crew_max_hours,
                "route_limit": route_limit,
                "taxi_buffer_minutes": taxi_buffer_minutes,
            },
            "fleet_overview": fleet_overview,
            "summary": summary,
            "assignments": assignments,
            "unassigned": unassigned,
            "tail_logs": tail_logs,
        }

    def summarize_asm_sources(self, cost_df):
        """Provide an accuracy snapshot for ASM calculations by seat source."""
        if cost_df is None or cost_df.empty:
            return pd.DataFrame(
                columns=[
                    "Seat Source",
                    "Routes",
                    "Valid ASM Routes",
                    "Total Seats",
                    "Total ASM",
                    "ASM Share",
                ]
            )

        df = cost_df.copy()
        df["Seat Source"] = df.get("Seat Source", "unknown").fillna("unknown")
        df["Total Seats"] = pd.to_numeric(df.get("Total Seats"), errors="coerce").fillna(0.0)
        df["ASM"] = pd.to_numeric(df.get("ASM"), errors="coerce").fillna(0.0)
        df["ASM Valid"] = df.get("ASM Valid", False).fillna(False)

        grouped = (
            df.groupby("Seat Source", dropna=False)
            .agg(
                Routes=("Seat Source", "count"),
                Valid_ASM_Routes=("ASM Valid", lambda values: int(pd.Series(values).fillna(False).sum())),
                Total_Seats=("Total Seats", "sum"),
                Total_ASM=("ASM", "sum"),
            )
            .reset_index()
        )
        grouped = grouped.rename(
            columns={
                "Valid_ASM_Routes": "Valid ASM Routes",
                "Total_Seats": "Total Seats",
                "Total_ASM": "Total ASM",
            }
        )

        asm_total = grouped["Total ASM"].sum()
        grouped["ASM Share Value"] = grouped["Total ASM"] / asm_total if asm_total > 0 else 0.0

        grouped["Total Seats"] = grouped["Total Seats"].round(0).astype(int)
        grouped["Total ASM"] = grouped["Total ASM"].round(0).astype(int)
        grouped["ASM Share"] = grouped["ASM Share Value"].apply(lambda value: f"{value * 100:.1f}%")

        ordered_columns = [
            "Seat Source",
            "Routes",
            "Valid ASM Routes",
            "Total Seats",
            "Total ASM",
            "ASM Share",
            "ASM Share Value",
        ]

        return grouped[ordered_columns].sort_values("Total ASM", ascending=False).reset_index(drop=True)

    def detect_asm_alerts(self, summary_df, estimate_threshold=0.4, unknown_threshold=0.1):
        """Highlight when estimated or unknown seat sources dominate ASM."""
        alerts = []
        if summary_df is None or summary_df.empty:
            return alerts

        share_lookup = {
            row["Seat Source"]: float(row.get("ASM Share Value", 0) or 0)
            for _, row in summary_df.iterrows()
        }

        estimate_share = share_lookup.get("equipment_estimate", 0.0)
        unknown_share = share_lookup.get("unknown", 0.0)

        if estimate_share >= estimate_threshold:
            alerts.append(
                f"{estimate_share:.0%} of ASM relies on equipment estimates; refresh aircraft configs."
            )
        if unknown_share >= unknown_threshold:
            alerts.append(
                f"{unknown_share:.0%} of ASM lacks seat data (unknown source)."
            )

        combined = estimate_share + unknown_share
        dominant_threshold = max(estimate_threshold, unknown_threshold)
        if combined >= dominant_threshold and not alerts:
            alerts.append(
                f"{combined:.0%} of ASM is estimated or unknown; investigate data coverage."
            )

        return alerts

    def simulate_cbsa_route_opportunities(self, airline_cost_df, top_n=10, max_suggestions_per_route=3):
        """
        Identify an airline's best-performing routes and suggest new CBSA-similar opportunities.
        Returns a dictionary with DataFrames for the top routes and suggested new routes.
        """
        if airline_cost_df is None or airline_cost_df.empty:
            return {
                "best_routes": pd.DataFrame(),
                "suggested_routes": pd.DataFrame()
            }

        source_codes = airline_cost_df["Source airport"].dropna().unique().tolist()
        dest_codes = airline_cost_df["Destination airport"].dropna().unique().tolist()
        required_iatas = list({code for code in source_codes + dest_codes if isinstance(code, str)})
        if not required_iatas:
            return {
                "best_routes": pd.DataFrame(),
                "suggested_routes": pd.DataFrame()
            }

        airport_subset = self.airports[self.airports["IATA"].isin(required_iatas)]
        airport_cbsa = self.annotate_airports_with_cbsa(airport_subset).copy()
        if airport_cbsa.empty:
            return {
                "best_routes": pd.DataFrame(),
                "suggested_routes": pd.DataFrame()
            }

        airport_cbsa["Country"] = airport_cbsa["Country"].astype(str).str.strip()
        airport_cbsa["CBSA Name"] = (
            airport_cbsa["CBSA Name"]
            .fillna("")
            .astype(str)
            .str.strip()
        )
        airport_cbsa = airport_cbsa[
            (airport_cbsa["Country"] == "United States") &
            (airport_cbsa["CBSA Name"] != "")
        ].copy()

        if airport_cbsa.empty:
            return {
                "best_routes": pd.DataFrame(),
                "suggested_routes": pd.DataFrame()
            }

        airport_cbsa["__cbsa_key"] = airport_cbsa["CBSA Name"].str.lower()
        airport_cbsa_for_merge = airport_cbsa.drop(columns="__cbsa_key")

        source_columns = {
            "IATA": "Source airport",
            "Name": "Source Name",
            "City": "Source City",
            "Country": "Source Country",
            "Latitude": "Source Latitude",
            "Longitude": "Source Longitude",
            "CBSA Name": "Source CBSA Name",
            "CBSA Code": "Source CBSA Code",
            "County/County Equivalent": "Source County",
            "State Name": "Source State"
        }
        dest_columns = {
            "IATA": "Destination airport",
            "Name": "Destination Name",
            "City": "Destination City",
            "Country": "Destination Country",
            "Latitude": "Destination Latitude",
            "Longitude": "Destination Longitude",
            "CBSA Name": "Destination CBSA Name",
            "CBSA Code": "Destination CBSA Code",
            "County/County Equivalent": "Destination County",
            "State Name": "Destination State"
        }

        routes_with_cbsa = airline_cost_df.merge(
            airport_cbsa_for_merge.rename(columns=source_columns),
            on="Source airport",
            how="left"
        )
        routes_with_cbsa = routes_with_cbsa.merge(
            airport_cbsa_for_merge.rename(columns=dest_columns),
            on="Destination airport",
            how="left"
        )
        if "Route Strategy Baseline" not in routes_with_cbsa.columns:
            routes_with_cbsa["Route Strategy Baseline"] = pd.NA
        for column in ["Competition Score", "Route Maturity Score", "Yield Proxy Score"]:
            if column not in routes_with_cbsa.columns:
                routes_with_cbsa[column] = pd.NA

        group_cols = [
            "Source airport",
            "Source Name",
            "Source City",
            "Source Country",
            "Source CBSA Name",
            "Destination airport",
            "Destination Name",
            "Destination City",
            "Destination Country",
            "Destination CBSA Name"
        ]

        aggregated = (
            routes_with_cbsa.groupby(group_cols, dropna=False, as_index=False)
            .agg({
                "ASM": "sum",
                "Total Seats": "sum",
                "Distance (miles)": "mean",
                "Seats per Mile": "mean",
                "Route Strategy Baseline": "mean",
                "Competition Score": "mean",
                "Route Maturity Score": "mean",
                "Yield Proxy Score": "mean"
            })
        )

        if aggregated.empty:
            return {
                "best_routes": pd.DataFrame(),
                "suggested_routes": pd.DataFrame()
            }

        # CBSA metadata only exists for U.S. airports, so keep routes where both ends retain CBSA info.
        aggregated["__source_cbsa_valid"] = aggregated["Source CBSA Name"].apply(
            lambda value: isinstance(value, str) and value.strip() != ""
        )
        aggregated["__dest_cbsa_valid"] = aggregated["Destination CBSA Name"].apply(
            lambda value: isinstance(value, str) and value.strip() != ""
        )
        aggregated = aggregated[
            aggregated["__source_cbsa_valid"] & aggregated["__dest_cbsa_valid"]
        ].copy()
        aggregated.drop(columns=["__source_cbsa_valid", "__dest_cbsa_valid"], inplace=True)

        aggregated["__source_domestic"] = aggregated["Source Country"].astype(str).str.strip().str.lower() == "united states"
        aggregated["__dest_domestic"] = aggregated["Destination Country"].astype(str).str.strip().str.lower() == "united states"
        aggregated = aggregated[
            aggregated["__source_domestic"] & aggregated["__dest_domestic"]
        ].copy()
        aggregated.drop(columns=["__source_domestic", "__dest_domestic"], inplace=True)

        if aggregated.empty:
            return {
                "best_routes": pd.DataFrame(),
                "suggested_routes": pd.DataFrame()
            }

        asm_max = aggregated["ASM"].max()
        spm_max = aggregated["Seats per Mile"].max()
        asm_total = aggregated["ASM"].sum()
        aggregated["ASM_norm"] = aggregated["ASM"] / asm_max if asm_max and asm_max > 0 else 0
        aggregated["SPM_norm"] = aggregated["Seats per Mile"] / spm_max if spm_max and spm_max > 0 else 0
        aggregated["Performance Score"] = (
            0.7 * aggregated["ASM_norm"].fillna(0) +
            0.3 * aggregated["SPM_norm"].fillna(0)
        )

        best_routes = aggregated.sort_values("Performance Score", ascending=False).head(top_n).reset_index(drop=True)
        best_routes["Route"] = best_routes["Source airport"] + "->" + best_routes["Destination airport"]
        if asm_total and asm_total > 0:
            best_routes["ASM Share"] = best_routes["ASM"] / asm_total
        else:
            best_routes["ASM Share"] = 0.0

        existing_pairs = set(
            zip(
                routes_with_cbsa["Source airport"],
                routes_with_cbsa["Destination airport"]
            )
        )

        cbsa_airport_cache = {}

        def _normalize_cbsa(value):
            if not isinstance(value, str):
                return ""
            return value.strip().lower()

        def _round_numeric_columns(df, columns, digits=2):
            for col in columns:
                if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].round(digits)

        def _score_descriptor(value, strong_label, steady_label, watch_label):
            if pd.isna(value):
                return None
            if value >= 0.8:
                return strong_label
            if value >= 0.6:
                return steady_label
            return watch_label

        def _route_rationale(row):
            snippets = []
            perf = row.get("Performance Score")
            if pd.notna(perf):
                snippets.append(f"Score {perf:.2f}")

            yield_score = row.get("Yield Proxy Score")
            yield_phrase = _score_descriptor(yield_score, "Premium yields", "Steady yields", "Developing yields")
            if yield_phrase:
                snippets.append(f"{yield_phrase} ({yield_score:.2f})")

            competition_score = row.get("Competition Score")
            competition_phrase = _score_descriptor(
                competition_score,
                "Light competition",
                "Balanced competition",
                "Crowded field",
            )
            if competition_phrase:
                snippets.append(f"{competition_phrase} ({competition_score:.2f})")

            maturity_score = row.get("Route Maturity Score")
            maturity_phrase = _score_descriptor(
                maturity_score,
                "Established demand",
                "Maturing demand",
                "Emerging demand",
            )
            if maturity_phrase:
                snippets.append(f"{maturity_phrase} ({maturity_score:.2f})")

            asm_share = row.get("ASM Share")
            if pd.notna(asm_share) and asm_share > 0:
                snippets.append(f"{asm_share * 100:.1f}% of ASM")

            trimmed = [snippet for snippet in snippets if snippet][:4]
            if not trimmed:
                return "Reliable CBSA performer."
            return " • ".join(trimmed)

        best_routes["Route Rationale"] = best_routes.apply(_route_rationale, axis=1)

        def get_airports_for_cbsa(cbsa_name):
            normalized = _normalize_cbsa(cbsa_name)
            if not normalized:
                return pd.DataFrame()
            if normalized in cbsa_airport_cache:
                return cbsa_airport_cache[normalized]

            matches = airport_cbsa[airport_cbsa["__cbsa_key"] == normalized]
            cbsa_airport_cache[normalized] = matches
            return matches

        suggestions = []
        for _, route in best_routes.iterrows():
            source_cbsa_name = route.get("Source CBSA Name")
            dest_cbsa_name = route.get("Destination CBSA Name")

            if not source_cbsa_name or not dest_cbsa_name:
                continue

            source_candidates = get_airports_for_cbsa(source_cbsa_name)
            dest_candidates = get_airports_for_cbsa(dest_cbsa_name)

            if source_candidates.empty or dest_candidates.empty:
                continue

            suggestions_added = 0
            for _, source_candidate in source_candidates.iterrows():
                for _, dest_candidate in dest_candidates.iterrows():
                    proposed_pair = (source_candidate["IATA"], dest_candidate["IATA"])

                    if proposed_pair in existing_pairs:
                        continue
                    if proposed_pair == (route["Source airport"], route["Destination airport"]):
                        continue
                    if source_candidate.get("Country") != "United States" or dest_candidate.get("Country") != "United States":
                        continue
                    if pd.isna(source_candidate["Latitude"]) or pd.isna(dest_candidate["Latitude"]):
                        continue
                    if pd.isna(source_candidate["Longitude"]) or pd.isna(dest_candidate["Longitude"]):
                        continue
                    if pd.isna(source_candidate["CBSA Name"]) or pd.isna(dest_candidate["CBSA Name"]):
                        continue

                    distance = geodesic(
                        (source_candidate["Latitude"], source_candidate["Longitude"]),
                        (dest_candidate["Latitude"], dest_candidate["Longitude"])
                    ).miles

                    baseline_distance = route.get("Distance (miles)")
                    similarity_score = None
                    if pd.notna(baseline_distance) and baseline_distance > 0:
                        similarity_score = max(
                            0.0,
                            1 - abs(distance - baseline_distance) / baseline_distance
                        )

                    reference_score = route.get("Performance Score", 0) or 0
                    distance_factor = similarity_score if similarity_score is not None else 0.5
                    opportunity_score = reference_score * (0.5 + 0.5 * distance_factor)
                    rounded_opportunity_score = round(opportunity_score, 2)
                    rounded_similarity = round(similarity_score, 2) if similarity_score is not None else None
                    rounded_distance = round(distance, 2)
                    rounded_reference_score = round(reference_score, 2)
                    baseline_reference = route.get("Route Strategy Baseline")
                    rounded_baseline = round(baseline_reference, 2) if pd.notna(baseline_reference) else None
                    rounded_competition = round(route.get("Competition Score", 0) or 0, 2)
                    rounded_maturity = round(route.get("Route Maturity Score", 0) or 0, 2)
                    rounded_yield = round(route.get("Yield Proxy Score", 0) or 0, 2)

                    suggestions.append({
                        "Proposed Source": source_candidate["IATA"],
                        "Proposed Destination": dest_candidate["IATA"],
                        "Proposed Source City": source_candidate["City"],
                        "Proposed Destination City": dest_candidate["City"],
                        "Source CBSA": source_cbsa_name,
                        "Destination CBSA": dest_cbsa_name,
                        "Reference Route": route["Route"],
                        "Reference ASM": route["ASM"],
                        "Reference Performance Score": rounded_reference_score,
                        "Reference Baseline Score": rounded_baseline,
                        "Reference Competition Score": rounded_competition,
                        "Reference Maturity Score": rounded_maturity,
                        "Reference Yield Score": rounded_yield,
                        "Estimated Distance (miles)": rounded_distance,
                        "Distance Similarity": rounded_similarity,
                        "Opportunity Score": rounded_opportunity_score,
                        "Rationale": f"Shares CBSA pair {source_cbsa_name} <-> {dest_cbsa_name} with top route {route['Route']}"
                    })

                    suggestions_added += 1
                    if suggestions_added >= max_suggestions_per_route:
                        break
                if suggestions_added >= max_suggestions_per_route:
                    break

        suggestions_df = pd.DataFrame(suggestions)
        if not suggestions_df.empty:
            suggestions_df = (
                suggestions_df
                .drop_duplicates(subset=["Proposed Source", "Proposed Destination", "Source CBSA", "Destination CBSA"])
                .sort_values(["Opportunity Score", "Reference ASM"], ascending=[False, False])
                .reset_index(drop=True)
            )
        display_columns = [
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

        if not best_routes.empty:
            _round_numeric_columns(
                best_routes,
                [
                    "ASM",
                    "Total Seats",
                    "Distance (miles)",
                    "Seats per Mile",
                    "Route Strategy Baseline",
                    "Competition Score",
                    "Route Maturity Score",
                    "Yield Proxy Score",
                    "ASM_norm",
                    "SPM_norm",
                    "Performance Score",
                ],
            )
            best_routes_display = best_routes[display_columns].copy()
        else:
            best_routes_display = best_routes.reindex(columns=display_columns)

        if not best_routes_display.empty:
            best_routes_display = best_routes_display.drop(
                columns=["Source CBSA Name", "Destination CBSA Name"],
                errors="ignore",
            )
            if "ASM Share" in best_routes_display.columns:
                best_routes_display["ASM Share"] = best_routes_display["ASM Share"].apply(
                    lambda value: f"{value * 100:.1f}%" if pd.notna(value) else None
                )

        if not suggestions_df.empty:
            _round_numeric_columns(
                suggestions_df,
                [
                    "Reference ASM",
                    "Reference Performance Score",
                    "Reference Baseline Score",
                    "Reference Competition Score",
                    "Reference Maturity Score",
                    "Reference Yield Score",
                    "Estimated Distance (miles)",
                    "Distance Similarity",
                    "Opportunity Score",
                ],
            )
            suggestions_df = suggestions_df.drop(columns=["Source CBSA", "Destination CBSA"], errors="ignore")

        return {
            "best_routes": best_routes_display,
            "suggested_routes": suggestions_df
        }

    def find_competing_routes(self, airline_x_df, airline_y_df):
        """Find competing routes between two airlines."""
        ## Merge the two DataFrames on source and destination airports
        merged_df = pd.merge(
            airline_x_df,
            airline_y_df,
            on=["Source airport", "Destination airport"],
            suffixes=('_x', '_y')
        )

        ## Filter for routes where both airlines have flights
        competing_routes = merged_df[
            (merged_df['Airline (Normalized)_x'].notna()) & (merged_df['Airline (Normalized)_y'].notna())
        ]

        def _airline_label(row, airline_key, normalized_key):
            if airline_key in row and pd.notna(row[airline_key]):
                return row[airline_key]
            return row[normalized_key]

        long_format = pd.DataFrame([
            {
                "Source": row["Source airport"],
                "Dest": row["Destination airport"],
                _airline_label(row, "Airline_x", "Airline (Normalized)_x"): row["ASM_x"],
                _airline_label(row, "Airline_y", "Airline (Normalized)_y"): row["ASM_y"],
            }
            for _, row in competing_routes.iterrows()
        ])
        
        aircraft_info = pd.DataFrame([
            {
                "Source": row["Source airport"],
                "Dest": row["Destination airport"],
                f"{_airline_label(row, 'Airline_x', 'Airline (Normalized)_x')}_Aircraft": row["Equipment_x"],
                f"{_airline_label(row, 'Airline_y', 'Airline (Normalized)_y')}_Aircraft": row["Equipment_y"],
            }
            for _, row in competing_routes.iterrows()
        ])

        ## Melt and pivot to get airline names as columns
        melted = long_format.melt(id_vars=["Source", "Dest"], var_name="Airline", value_name="ASM")
        pivoted = melted.pivot_table(index=["Source", "Dest"], columns="Airline", values="ASM").reset_index()
        pivoted = pivoted.merge(aircraft_info, on=["Source", "Dest"], how="left")
        pivoted.columns.name = None  # remove column index name

        # Rename columns to be human-friendly and format numeric values.
        asm_columns = [
            col for col in pivoted.columns
            if col not in {"Source", "Dest"} and not col.endswith("_Aircraft")
        ]

        route_pairs = list(zip(pivoted["Source"], pivoted["Dest"]))
        route_total_lookup = self.get_route_total_asm(route_pairs)
        pivoted["_combined_asm"] = pivoted[asm_columns].sum(axis=1, min_count=1)
        pivoted["_route_total_asm"] = [route_total_lookup.get(pair) for pair in route_pairs]
        pivoted["_share_denominator"] = pivoted["_route_total_asm"]
        pivoted.loc[
            pivoted["_share_denominator"].isna() | (pivoted["_share_denominator"] <= 0),
            "_share_denominator"
        ] = pd.NA

        share_columns = []
        share_column_map = {}
        share_label_lookup = {}
        for col in asm_columns:
            share_col = f"{col}_Share"
            share_columns.append(share_col)
            pivoted[share_col] = pivoted[col] / pivoted["_share_denominator"]
            pivoted[share_col] = pivoted[share_col].clip(upper=1.0)
            share_column_map[share_col] = f"{col} ASM Share"
            share_label_lookup[col] = share_column_map[share_col]

        pivoted.drop(columns=asm_columns + ["_combined_asm", "_route_total_asm", "_share_denominator"], inplace=True)

        aircraft_suffix = "_Aircraft"

        def _aircraft_label(column_name: str) -> str:
            if column_name.endswith(aircraft_suffix):
                base = column_name[:-len(aircraft_suffix)]
            else:
                base = column_name
            return f"{base} Aircraft"

        aircraft_label_map = {
            col: _aircraft_label(col)
            for col in pivoted.columns if col.endswith(aircraft_suffix)
        }

        rename_map = {**share_column_map, **aircraft_label_map}
        formatted = pivoted.rename(columns=rename_map)

        def _format_share(value):
            if pd.isna(value):
                return None
            return f"{value * 100:.1f}%"

        for col in (share_column_map[share_col] for share_col in share_columns if share_col in share_column_map):
            if col in formatted.columns:
                formatted[col] = formatted[col].apply(_format_share)

        ordered_columns = ["Source", "Dest"]
        for original in asm_columns:
            share_label = share_label_lookup.get(original)
            if share_label and share_label in formatted.columns:
                ordered_columns.append(share_label)
            aircraft_original = f"{original}_Aircraft"
            aircraft_col = aircraft_label_map.get(aircraft_original)
            if aircraft_col and aircraft_col in formatted.columns:
                ordered_columns.append(aircraft_col)

        remaining = [col for col in formatted.columns if col not in ordered_columns]
        if remaining:
            ordered_columns.extend(remaining)

        return formatted[ordered_columns]


            
        
