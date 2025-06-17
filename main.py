from src.load_data import DataStore

def main():
    data_storage = DataStore()
    data_storage.load_data()
    Airline_x = data_storage.user_airline()
    Airline_y = data_storage.user_airline()

    Airline_x_df = data_storage.process_routes(Airline_x)
    Airline_y_df = data_storage.process_routes(Airline_y)

    Airline_x_df = data_storage.cost_analysis(Airline_x_df)
    Airline_y_df = data_storage.cost_analysis(Airline_y_df)

    competing_routes = data_storage.find_competing_routes(Airline_x_df, Airline_y_df)
    print(competing_routes.head(10))

    Airline_x_Network = data_storage.build_network(Airline_x_df)
    Airline_y_Network = data_storage.build_network(Airline_y_df)

    Airline_x_Network = data_storage.analyze_network(Airline_x_Network)
    Airline_y_Network = data_storage.analyze_network(Airline_y_Network)
    ## data_storage.draw_network(Airline_Network)
    print(f"Network analysis for Airlines")
    print("\nAirline X Network:")
    for key, value in Airline_x_Network.items():
        print(f"{key}: {value}")
    print("\n")
    print("Airline Y Network:")
    for key, value in Airline_y_Network.items():
        print(f"{key}: {value}")
    
if __name__ == "__main__":
    main()
