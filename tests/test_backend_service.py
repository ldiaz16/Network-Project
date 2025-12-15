import pandas as pd

from src.backend_service import AllianceAnalysisRequest, alliance_analysis, get_airline_fleet_profile, list_alliances
from src.load_data import DataStore
from tests.helpers import seed_sample_airline


def test_get_airline_fleet_profile_serializes_metadata_dict():
    datastore = DataStore()
    seed_sample_airline(datastore)

    profile = get_airline_fleet_profile(datastore, "Sample Airways")

    assert profile["airline"]["iata"] == "SA"
    assert profile["airline"]["total_routes"] == 5
    assert profile["network_stats"]["Number of Routes Flown"] == 5
    assert len(profile["top_routes"]) == 5


def test_alliance_analysis_aggregates_carrier_group_network():
    datastore = DataStore()
    datastore.carrier_group_lookup = {2: "National Carriers", 3: "Major Carriers"}
    datastore.airlines = pd.DataFrame(
        [
            {"Airline": "Alpha Air", "IATA": "AA", "Country": "United States"},
            {"Airline": "Unified", "IATA": "UA", "Country": "United States"},
            {"Airline": "Regional", "IATA": "RG", "Country": "United States"},
        ]
    )
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "JFK", "Name": "John F. Kennedy", "Latitude": 40.6413, "Longitude": -73.7781, "Country": "United States"},
            {"IATA": "LAX", "Name": "Los Angeles", "Latitude": 33.9416, "Longitude": -118.4085, "Country": "United States"},
            {"IATA": "SFO", "Name": "San Francisco", "Latitude": 37.6213, "Longitude": -122.3790, "Country": "United States"},
        ]
    )
    datastore.routes = pd.DataFrame(
        [
            {
                "Airline Code": "AA",
                "Source airport": "JFK",
                "Destination airport": "LAX",
                "Total": 100.0,
                "Passengers": 80.0,
                "Distance (reported)": 2475.0,
                "Equipment": "A320",
                "Seat Source": "t100_segments",
                "Carrier Group": 3,
            },
            {
                "Airline Code": "UA",
                "Source airport": "LAX",
                "Destination airport": "SFO",
                "Total": 50.0,
                "Passengers": 40.0,
                "Distance (reported)": 337.0,
                "Equipment": "B738",
                "Seat Source": "t100_segments",
                "Carrier Group": 3,
            },
            {
                "Airline Code": "RG",
                "Source airport": "JFK",
                "Destination airport": "SFO",
                "Total": 10.0,
                "Passengers": 8.0,
                "Distance (reported)": 2586.0,
                "Equipment": "CR9",
                "Seat Source": "t100_segments",
                "Carrier Group": 2,
            },
        ]
    )

    alliances = list_alliances(datastore)
    assert {entry["code"] for entry in alliances} == {2, 3}

    result = alliance_analysis(datastore, AllianceAnalysisRequest(carrier_group=3, limit=10))
    assert result["alliance"]["name"] == "Major Carriers"
    assert result["summary"]["member_carriers"] == 2
    assert result["summary"]["total_routes"] == 2
    assert len(result["top_carriers"]) == 2
