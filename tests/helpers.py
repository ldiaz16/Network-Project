import pandas as pd

from data.airlines import normalize_name


def seed_sample_airline(datastore):
    """Populate the provided datastore with a minimal airline, route, and airport dataset."""
    datastore.airlines = pd.DataFrame(
        [
            {
                "Airline": "Sample Airways",
                "Alias": "",
                "IATA": "SA",
                "ICAO": "SMP",
                "Callsign": "SAMPLE",
                "Country": "United States",
                "Active": "Y",
                "Airline (Normalized)": normalize_name("Sample Airways"),
            }
        ]
    )
    datastore.routes = pd.DataFrame(
        [
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "AAA",
                "Source airport ID": None,
                "Destination airport": "BBB",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A320",
            },
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "AAA",
                "Source airport ID": None,
                "Destination airport": "CCC",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A320",
            },
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "BBB",
                "Source airport ID": None,
                "Destination airport": "DDD",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A321",
            },
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "AAA",
                "Source airport ID": None,
                "Destination airport": "DDD",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A321",
            },
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "BBB",
                "Source airport ID": None,
                "Destination airport": "CCC",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "CR9",
            },
        ]
    )
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "AAA", "Name": "Airport A", "Latitude": 0.0, "Longitude": 0.0},
            {"IATA": "BBB", "Name": "Airport B", "Latitude": 0.0, "Longitude": 10.0},
            {"IATA": "CCC", "Name": "Airport C", "Latitude": 5.0, "Longitude": 15.0},
            {"IATA": "DDD", "Name": "Airport D", "Latitude": 15.0, "Longitude": 30.0},
        ]
    )
    config = {
        "Sample Airways": {
            "A320": {"Y": 150, "W": 0, "J": 20, "F": 0, "Total": 170},
            "A321": {"Y": 180, "W": 0, "J": 20, "F": 0, "Total": 200},
            "CR9": {"Y": 70, "W": 0, "J": 6, "F": 0, "Total": 76},
        }
    }
    datastore.aircraft_config = datastore.convert_aircraft_config_to_df(config)
