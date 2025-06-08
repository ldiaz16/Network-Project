import pandas as pd
from data.airlines import normalize_name
from rapidfuzz import process, fuzz # type: ignore

class DataStore:
    def __init__(self, data_dir="data/"):
        self.data_dir = data_dir
        self.routes = None
        self.airlines = None
        self.airports = None

    def load_data(self):

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
        matched_airlines = process.extract(norm_airline, self.airlines["Airline (Normalized)"], scorer=fuzz.partial_ratio, limit=20)
        best_score = 0
        best_match = None

        for name, score, index in matched_airlines:
            row = self.airlines[self.airlines["Airline (Normalized)"] == name].iloc[0]
            Airline_IATA = row['IATA']
            airline_routes = self.calculate_total_airline_routes(Airline_IATA)
            row["Total Routes"] = airline_routes
            ## print(f"Matched airline: {row['Airline']} with IATA code {row['IATA']} and {row['Total Routes']} routes and score {score}.")
            if pd.notna(row['IATA']) and score > best_score :
                best_score = airline_routes
                best_match = row
              

        ## Create a dataframe with the airlines information whose name matches input 
        matched_airline = best_match
        print(f"Best match: {matched_airline['Airline']} with IATA code {matched_airline['IATA']} and {matched_airline['Total Routes']} routes.")

    
    def calculate_total_airline_routes(self, airline_code):
        if not isinstance(airline_code, str) or airline_code.strip() == "":
            return 0
        
        filtered = self.routes[self.routes['Airline Code'].str.lower() == airline_code.lower()]
        return len(filtered)
   