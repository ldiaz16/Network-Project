import pandas as pd

class DataStore:
    def __init__(self, data_dir="../data/"):
        self.data_dir = data_dir
        self.routes = None
        self.airlines = None
        self.airports = None

    def load_data(self):
        self.routes = pd.read_csv(f"{self.data_dir}routes.dat", header=None)
        self.airlines = pd.read_csv(f"{self.data_dir}airlines.dat", header=None)
        self.airports = pd.read_csv(f"{self.data_dir}airports.dat", header=None)

        # Drop the first column which is not needed
        self.airlines.drop(columns=[0], inplace=True) 

        self.routes.columns = [
            "Airline", "Airline ID", "Source airport", "Source airport ID",
            "Destination airport", "Destination airport ID", "Codeshare", "Stops", "Equipment"
        ]
        self.airlines.columns = [
            "Airline", "Alias", "IATA", "ICAO", "Callsign", "Country", "Active"
        ]
        self.airports.columns = [
            "Airport ID", "Name", "City", "Country", "IATA", "ICAO",
            "Latitude", "Longitude", "Altitude", "Timezone", "DST", "Tz", "Type", "Source"
        ]

def main():
    data_storage = DataStore()
    data_storage.load_data()

    print("Routes:")
    print(data_storage.routes.head())

    print("\nAirlines:")
    print(data_storage.airlines.head())

    print("\nAirports:")
    print(data_storage.airports.head())

if __name__ == "__main__":
    main()
