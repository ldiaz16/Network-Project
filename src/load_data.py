import sys
import os
import pandas as pd
from data.airlines import normalize_name
import networkx as nx
from rapidfuzz import process, fuzz # type: ignore


class DataStore:
    def __init__(self, data_dir="data/"):
        self.data_dir = data_dir
        self.routes = None
        self.airlines = None
        self.airports = None

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


    def user_airline(self):
        """Prompt user for an airline and display the number of routes for that airline."""

        input_airline = input("Enter an airline: ")
        norm_airline = normalize_name(input_airline)

        ## Use fuzzy matching on the normalized airline column
        matched_airlines = process.extract(norm_airline, self.airlines["Airline (Normalized)"], scorer=fuzz.WRatio, limit=20)
        best_score = 0
        best_match = None

        alt_score = 0
        alt_match = None

        for name, score, index in matched_airlines:
            row = self.airlines[self.airlines["Airline (Normalized)"] == name].iloc[0]
            Airline_IATA = row['IATA']
            airline_routes = self.calculate_total_airline_routes(Airline_IATA)

            ## add row for total routes
            row["Total Routes"] = airline_routes

            ## print(f"Matched airline: {row['Airline']} with IATA code {row['IATA']} and {row['Total Routes']} routes and score {score}.")
            if pd.notna(row['IATA']) and score >= best_score :
                alt_score = best_score
                alt_match = best_match
                best_score = airline_routes
                best_match = row
            elif score > alt_score:
                alt_score = airline_routes
                alt_match = row
              
        if alt_match['Airline (Normalized)'] == norm_airline:
            best_match = alt_match

        ## Create a dataframe with the airlines information whose name matches input 
        matched_airline = best_match
        matched_airline_IATA = matched_airline['IATA']
        print(f"Best match: {matched_airline['Airline']} with IATA code {matched_airline['IATA']} and {matched_airline['Total Routes']} routes.")
        
        ## Get the DataFrame of routes for the matched airline
        user_airline_routes_df = self.routes[self.routes['Airline Code'].str.lower() == matched_airline_IATA.lower()]
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
        """Process the routes DataFrame to extract relevant information."""
        for row in routes_df.itertuples(index=False):
            source_airport = self.airports[self.airports['IATA'] == row[2]]
            destination_airport = self.airports[self.airports['IATA'] == row[4]]
            source_name = source_airport['Name'].values[0] if not source_airport.empty else "Unknown"
            destination_name = destination_airport['Name'].values[0] if not destination_airport.empty else "Unknown"
            print(f"Processing route: {source_name} to {destination_name}")
          
    
