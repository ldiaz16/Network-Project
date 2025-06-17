import sys
import os
import pandas as pd
from data.airlines import normalize_name
import networkx as nx
from rapidfuzz import process, fuzz # type: ignore
from geopy.distance import geodesic
from data.aircraft_config import AIRLINE_SEAT_CONFIG
from collections import defaultdict



class DataStore:
    def __init__(self, data_dir="data/"):
        self.data_dir = data_dir
        self.routes = None
        self.airlines = None
        self.airports = None
        self.aircraft_config = None

    def load_data(self):
        """Load data from CSV files into DataFrames and preprocess them."""
        ## Create DataFrames for each dataset
        self.routes = pd.read_csv(f"{self.data_dir}routes.dat", header=None)
        self.airlines = pd.read_csv(f"{self.data_dir}airlines.dat", header=None)
        self.airports = pd.read_csv(f"{self.data_dir}airports.dat", header=None)

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


    def user_airline(self):
        """Prompt user for an airline and display the number of routes for that airline."""

        input_airline = input("Enter an airline: ")
        norm_airline = normalize_name(input_airline)

        ## Use fuzzy matching on the normalized airline column
        matched_airlines = process.extract(norm_airline, self.airlines["Airline (Normalized)"], scorer=fuzz.WRatio, limit=20)
        best_score = 0
        best_match = None
        highest_num_routes = 0

        for name, score, index in matched_airlines:
            row = self.airlines[self.airlines["Airline (Normalized)"] == name].iloc[0]
            Airline_IATA = row['IATA']
            airline_routes = self.calculate_total_airline_routes(Airline_IATA)

            ## add row for total routes
            row["Total Routes"] = airline_routes

            ## print(f"Matched airline: {row['Airline']} with IATA code {row['IATA']} and {row['Total Routes']} routes and score {score}.")
            if pd.notna(row['IATA']) and score == 100 and airline_routes > highest_num_routes:
                highest_num_routes = airline_routes
                best_match = row
                best_score = score
            elif pd.notna(row['IATA']) and score >= best_score :
                best_score = airline_routes
                best_match = row

        ## Create a dataframe with the airlines information whose name matches input 
        matched_airline = best_match
        matched_airline_IATA = matched_airline['IATA']

        matched_airline_name = matched_airline['Airline']
        matched_airline_name_normalized = matched_airline['Airline (Normalized)']

        print(f"Best match: {matched_airline['Airline']} with IATA code {matched_airline['IATA']} and {matched_airline['Total Routes']} routes.")
        ## Get the DataFrame of routes for the matched airline
        user_airline_routes_df = self.routes[self.routes['Airline Code'].str.lower() == matched_airline_IATA.lower()]
        user_airline_routes_df = user_airline_routes_df.copy()
        user_airline_routes_df['Airline'] = matched_airline_name
        user_airline_routes_df['Airline (Normalized)'] = matched_airline_name_normalized
        return user_airline_routes_df
    
    
    
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
            ## "Number of Aiports Flown To": num_airports,
            ## "Number of Routes Flown": num_routes,
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

        seats_on_route = self.aircraft_config[["Airline", "Aircraft", "Y", "W", "J", "F", "Total"]]
        
        # Merge the routes_df with the aircraft configuration
        routes_df = routes_df.merge(seats_on_route, left_on=["Airline (Normalized)", "Equipment"], right_on=["Airline", "Aircraft"], how="left")
        
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
    
    def cost_analysis(self, airline_df):
        """Perform cost analysis on the airline DataFrame."""
        # Calculate total seats available for each route
        airline_df["Total Seats"] = airline_df["Total"].fillna(0)

        """Calculate seats per mile for each route."""
        airline_df["Seats per Mile"] = airline_df.apply(
            lambda row: row["Total Seats"] / row["Distance (miles)"] if row["Distance (miles)"] > 0 else 0,
            axis=1
        )
        ## print(airline_df[["Airline", "Source airport", "Destination airport", "Distance (miles)", "Total Seats", "Seats per Mile"]].head())
        return airline_df[["Airline", "Airline (Normalized)", "Source airport", "Destination airport", "Distance (miles)", "Total Seats", "Seats per Mile"]]

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

        long_format = pd.DataFrame([
        {
            "Source": row["Source airport"],
            "Dest": row["Destination airport"],
            row["Airline (Normalized)_x"]: row["Seats per Mile_x"],
            row["Airline (Normalized)_y"]: row["Seats per Mile_y"],
        }
        for _, row in competing_routes.iterrows()
    ])

        ## Melt and pivot to get airline names as columns
        melted = long_format.melt(id_vars=["Source", "Dest"], var_name="Airline", value_name="Seats per Mile")
        pivoted = melted.pivot_table(index=["Source", "Dest"], columns="Airline", values="Seats per Mile").reset_index()
        pivoted.columns.name = None  # remove column index name

        print(f"Found {len(pivoted)} competing routes between the two airlines.")
        print("\nSeats per Mile for Competing Routes:")
        return pivoted


            
        
