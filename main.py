from src.load_data import DataStore

def main():
    data_storage = DataStore()
    data_storage.load_data()
    Airline_Routes = data_storage.user_airline()
    Airline_Network = data_storage.build_network(Airline_Routes)
    Network_Analysis = data_storage.analyze_network(Airline_Network)
    ## data_storage.draw_network(Airline_Network)
    data_storage.process_routes(Airline_Routes)
    for key, value in Network_Analysis.items():
        print(f"{key}: {value}")
    
if __name__ == "__main__":
    main()
