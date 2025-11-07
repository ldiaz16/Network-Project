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

DATA_DIR_ROOT = Path(__file__).resolve().parents[1] / "data"


class DataStore:
    def __init__(self, data_dir=None):
        base_dir = Path(data_dir) if data_dir else DATA_DIR_ROOT
        self.data_dir = base_dir
        self.routes = None
        self.airlines = None
        self.airports = None
        self.aircraft_config = None
        self.airports_metadata = None
        self.cbsa_cache = {}
        self._cbsa_cache_dirty = False
        self.cbsa_lookup = None
        self.cbsa_cache_path = self.data_dir / "cbsa_cache.json"
        self._load_cbsa_cache()

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

        best_score = 0
        best_match = None
        highest_num_routes = 0

        for name, score, index in matched_airlines:
            row = self.airlines[self.airlines["Airline (Normalized)"] == name].iloc[0]
            Airline_IATA = row['IATA']
            airline_routes = self.calculate_total_airline_routes(Airline_IATA)
            row = row.copy()
            row["Total Routes"] = airline_routes

            if pd.notna(row['IATA']) and score == 100 and airline_routes > highest_num_routes:
                highest_num_routes = airline_routes
                best_match = row
                best_score = score
            elif pd.notna(row['IATA']) and score >= best_score:
                best_score = score
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

        seats_on_route = self.aircraft_config.copy()
        
        # Merge the routes_df with the aircraft configuration
        routes_df = routes_df.merge(seats_on_route, left_on=["Airline (Normalized)", "Equipment"], right_on=["Airline", "Aircraft"], how="left")
        aircraft_list = routes_df["Equipment"].unique()
        ## print(aircraft_list)
        
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
    
    def find_best_aircraft_for_route(self, airline_name):
        aircraft_list_df = self.aircraft_config[self.aircraft_config["Airline"] == airline_name]

    
    def cost_analysis(self, airline_df):
        """Perform cost analysis on the airline DataFrame."""
        # Calculate total seats available for each route
        df = airline_df.copy()
        df["Total Seats"] = df["Total"].fillna(0)

        """Calculate seats per mile for each route."""
        df["Seats per Mile"] = df.apply(
            lambda row: row["Total Seats"] / row["Distance (miles)"] if row["Distance (miles)"] > 0 else 0,
            axis=1
        )

        df["ASM"] = df["Total Seats"] * df["Distance (miles)"]

        ## print(airline_df[["Airline", "Source airport", "Destination airport", "Distance (miles)", "Total Seats", "Seats per Mile"]].head())
        return df[["Airline", "Airline (Normalized)", "Source airport", "Destination airport", "Distance (miles)", "Total Seats", "Seats per Mile", "ASM", "Equipment"]]

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
            airport_cbsa.rename(columns=source_columns),
            on="Source airport",
            how="left"
        )
        routes_with_cbsa = routes_with_cbsa.merge(
            airport_cbsa.rename(columns=dest_columns),
            on="Destination airport",
            how="left"
        )

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
                "Seats per Mile": "mean"
            })
        )

        if aggregated.empty:
            return {
                "best_routes": pd.DataFrame(),
                "suggested_routes": pd.DataFrame()
            }

        asm_max = aggregated["ASM"].max()
        spm_max = aggregated["Seats per Mile"].max()
        aggregated["ASM_norm"] = aggregated["ASM"] / asm_max if asm_max and asm_max > 0 else 0
        aggregated["SPM_norm"] = aggregated["Seats per Mile"] / spm_max if spm_max and spm_max > 0 else 0
        aggregated["Performance Score"] = (
            0.7 * aggregated["ASM_norm"].fillna(0) +
            0.3 * aggregated["SPM_norm"].fillna(0)
        )

        best_routes = aggregated.sort_values("Performance Score", ascending=False).head(top_n).reset_index(drop=True)
        best_routes["Route"] = best_routes["Source airport"] + "-" + best_routes["Destination airport"]

        existing_pairs = set(
            zip(
                routes_with_cbsa["Source airport"],
                routes_with_cbsa["Destination airport"]
            )
        )

        cbsa_airport_cache = {}

        def get_airports_for_cbsa(cbsa_name):
            if not isinstance(cbsa_name, str):
                return pd.DataFrame()
            if cbsa_name in cbsa_airport_cache:
                return cbsa_airport_cache[cbsa_name]

            city_tokens = self._extract_cbsa_city_tokens(cbsa_name)
            if not city_tokens:
                cbsa_airport_cache[cbsa_name] = pd.DataFrame()
                return cbsa_airport_cache[cbsa_name]

            country_series = self.airports["Country"].astype(str).str.strip()
            city_series = self.airports["City"].astype(str).str.strip()
            candidate_subset = self.airports[
                (country_series == "United States") &
                (city_series.isin(city_tokens))
            ]
            annotated = self.annotate_airports_with_cbsa(candidate_subset)
            cbsa_airport_cache[cbsa_name] = annotated
            return annotated

        suggestions = []
        for _, route in best_routes.iterrows():
            source_cbsa_name = route.get("Source CBSA Name")
            dest_cbsa_name = route.get("Destination CBSA Name")

            if not source_cbsa_name or not dest_cbsa_name:
                continue

            source_candidates = get_airports_for_cbsa(source_cbsa_name)
            dest_candidates = get_airports_for_cbsa(dest_cbsa_name)

            if source_candidates.empty:
                source_candidates = airport_cbsa[airport_cbsa["CBSA Name"] == source_cbsa_name]
            if dest_candidates.empty:
                dest_candidates = airport_cbsa[airport_cbsa["CBSA Name"] == dest_cbsa_name]

            if source_candidates is None or dest_candidates is None:
                continue

            suggestions_added = 0
            for _, source_candidate in source_candidates.iterrows():
                for _, dest_candidate in dest_candidates.iterrows():
                    proposed_pair = (source_candidate["IATA"], dest_candidate["IATA"])

                    if proposed_pair in existing_pairs:
                        continue
                    if proposed_pair == (route["Source airport"], route["Destination airport"]):
                        continue
                    if pd.isna(source_candidate["Latitude"]) or pd.isna(dest_candidate["Latitude"]):
                        continue
                    if pd.isna(source_candidate["Longitude"]) or pd.isna(dest_candidate["Longitude"]):
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
                    domestic_bonus = 1.0
                    if source_candidate.get("Country") != "United States" or dest_candidate.get("Country") != "United States":
                        domestic_bonus = 0.85
                    opportunity_score = round(reference_score * (0.5 + 0.5 * distance_factor) * domestic_bonus, 3)

                    suggestions.append({
                        "Proposed Source": source_candidate["IATA"],
                        "Proposed Destination": dest_candidate["IATA"],
                        "Proposed Source City": source_candidate["City"],
                        "Proposed Destination City": dest_candidate["City"],
                        "Source CBSA": source_cbsa_name,
                        "Destination CBSA": dest_cbsa_name,
                        "Reference Route": route["Route"],
                        "Reference ASM": route["ASM"],
                        "Reference Performance Score": reference_score,
                        "Estimated Distance (miles)": round(distance, 1),
                        "Distance Similarity": round(similarity_score, 3) if similarity_score is not None else None,
                        "Opportunity Score": opportunity_score,
                        "Rationale": f"Shares CBSA pair {source_cbsa_name} ↔ {dest_cbsa_name} with top route {route['Route']}"
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
        return {
            "best_routes": best_routes,
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
        rename_map = {col: f"{col} ASM" for col in asm_columns}
        aircraft_suffix = "_Aircraft"

        def _aircraft_label(column_name: str) -> str:
            if column_name.endswith(aircraft_suffix):
                base = column_name[:-len(aircraft_suffix)]
            else:
                base = column_name
            return f"{base} Aircraft"

        rename_map.update({
            col: _aircraft_label(col)
            for col in pivoted.columns if col.endswith(aircraft_suffix)
        })
        formatted = pivoted.rename(columns=rename_map)

        def _format_asm(value):
            if pd.isna(value):
                return None
            if isinstance(value, (int, float)):
                return f"{value:,.0f}"
            return value

        for col in (rename_map[col] for col in asm_columns):
            formatted[col] = formatted[col].apply(_format_asm)

        ordered_columns = ["Source", "Dest"]
        for original in asm_columns:
            asm_col = rename_map[original]
            ordered_columns.append(asm_col)
            aircraft_original = f"{original}_Aircraft"
            aircraft_col = rename_map.get(aircraft_original)
            if aircraft_col and aircraft_col in formatted.columns:
                ordered_columns.append(aircraft_col)

        remaining = [col for col in formatted.columns if col not in ordered_columns]
        if remaining:
            ordered_columns.extend(remaining)

        return formatted[ordered_columns]


            
        
