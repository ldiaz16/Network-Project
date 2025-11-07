import pandas as pd
import pytest
from geopy.distance import geodesic

from data.airlines import normalize_name
from src.load_data import DataStore, GENERIC_SEAT_GUESSES


@pytest.fixture
def datastore():
    return DataStore()


def test_convert_aircraft_config_to_df_returns_expected_structure(datastore):
    config = {
        "Sample Airways": {
            "A320": {"Y": 150, "W": 10, "J": 12, "F": 4, "Total": 176},
            "A321": {"Y": 180, "W": 14, "J": 16, "F": 6, "Total": 216},
        }
    }

    df = datastore.convert_aircraft_config_to_df(config)

    expected = pd.DataFrame(
        [
            {
                "Airline": normalize_name("Sample Airways"),
                "Aircraft": "A320",
                "Y": 150,
                "W": 10,
                "J": 12,
                "F": 4,
                "Total": 176,
            },
            {
                "Airline": normalize_name("Sample Airways"),
                "Aircraft": "A321",
                "Y": 180,
                "W": 14,
                "J": 16,
                "F": 6,
                "Total": 216,
            },
        ]
    )
    expected.columns.name = "Cabin Class"

    pd.testing.assert_frame_equal(df, expected)


def test_calculate_total_airline_routes_handles_input_variants(datastore):
    columns = [
        "Airline Code",
        "IDK",
        "Source airport",
        "Source airport ID",
        "Destination airport",
        "Destination airport ID",
        "Codeshare",
        "Stops",
        "Equipment",
    ]
    datastore.routes = pd.DataFrame(
        [
            {"Airline Code": "AA", "Source airport": "JFK", "Destination airport": "LAX"},
            {"Airline Code": "aa", "Source airport": "LAX", "Destination airport": "SFO"},
            {"Airline Code": "BA", "Source airport": "LHR", "Destination airport": "JFK"},
        ],
        columns=columns,
    )

    assert datastore.calculate_total_airline_routes("AA") == 2
    assert datastore.calculate_total_airline_routes("aa") == 2
    assert datastore.calculate_total_airline_routes("") == 0
    assert datastore.calculate_total_airline_routes(None) == 0


def test_build_and_analyze_network(datastore):
    routes_df = pd.DataFrame(
        [
            {"Source airport": "JFK", "Destination airport": "LAX"},
            {"Source airport": "LAX", "Destination airport": "SFO"},
            {"Source airport": "SFO", "Destination airport": "LAX"},
        ]
    )

    graph = datastore.build_network(routes_df)
    metrics = datastore.analyze_network(graph)

    assert graph.number_of_edges() == 3
    assert graph.number_of_nodes() == 3
    assert metrics["Number of Aiports Flown To"] == 3
    assert metrics["Number of Routes Flown"] == 3
    assert metrics["Top 5 Hubs"][0] == ("LAX", 3)


def test_process_routes_enriches_with_distance_and_seats(datastore):
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "JFK", "Name": "John F. Kennedy", "Latitude": 40.6413, "Longitude": -73.7781},
            {"IATA": "LAX", "Name": "Los Angeles", "Latitude": 33.9416, "Longitude": -118.4085},
        ]
    )

    config = {
        "Sample Airways": {
            "A320": {"Y": 150, "W": 10, "J": 12, "F": 4, "Total": 176},
        }
    }
    datastore.aircraft_config = datastore.convert_aircraft_config_to_df(config)

    routes_df = pd.DataFrame(
        [
            {
                "Source airport": "JFK",
                "Destination airport": "LAX",
                "Airline (Normalized)": normalize_name("Sample Airways"),
                "Equipment": "A320",
            }
        ]
    )

    enriched = datastore.process_routes(routes_df)

    expected_distance = geodesic(
        (datastore.airports.loc[0, "Latitude"], datastore.airports.loc[0, "Longitude"]),
        (datastore.airports.loc[1, "Latitude"], datastore.airports.loc[1, "Longitude"]),
    ).miles

    assert enriched.loc[0, "Source Name"] == "John F. Kennedy"
    assert enriched.loc[0, "Destination Name"] == "Los Angeles"
    assert enriched.loc[0, "Total"] == 176
    assert enriched.loc[0, "Distance (miles)"] == pytest.approx(expected_distance, rel=1e-3)
    assert enriched.loc[0, "Distance (km)"] == pytest.approx(expected_distance * 1.60934, rel=1e-3)


def test_cost_analysis_computes_capacity_metrics(datastore):
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "JFK", "Name": "John F. Kennedy", "Latitude": 40.6413, "Longitude": -73.7781},
            {"IATA": "LAX", "Name": "Los Angeles", "Latitude": 33.9416, "Longitude": -118.4085},
        ]
    )

    config = {
        "Sample Airways": {
            "A320": {"Y": 150, "W": 10, "J": 12, "F": 4, "Total": 176},
        }
    }
    datastore.aircraft_config = datastore.convert_aircraft_config_to_df(config)

    routes_df = pd.DataFrame(
        [
            {
                "Source airport": "JFK",
                "Destination airport": "LAX",
                "Airline (Normalized)": normalize_name("Sample Airways"),
                "Equipment": "A320",
            }
        ]
    )

    enriched = datastore.process_routes(routes_df)
    analyzed = datastore.cost_analysis(enriched)

    distance_miles = enriched.loc[0, "Distance (miles)"]
    total_seats = enriched.loc[0, "Total"]

    assert analyzed.loc[0, "Total Seats"] == total_seats
    assert analyzed.loc[0, "Seats per Mile"] == pytest.approx(total_seats / distance_miles, rel=1e-3)
    assert analyzed.loc[0, "ASM"] == pytest.approx(total_seats * distance_miles, rel=1e-3)
    assert analyzed.loc[0, "Airline (Normalized)"] == normalize_name("Sample Airways")


def test_process_routes_estimates_seats_when_config_missing(datastore):
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "JFK", "Name": "John F. Kennedy", "Latitude": 40.6413, "Longitude": -73.7781},
            {"IATA": "LAX", "Name": "Los Angeles", "Latitude": 33.9416, "Longitude": -118.4085},
        ]
    )
    datastore.aircraft_config = pd.DataFrame(columns=["Airline", "Aircraft", "Y", "W", "J", "F", "Total"])
    datastore.equipment_capacity_lookup = datastore._build_equipment_capacity_lookup(datastore.aircraft_config)

    routes_df = pd.DataFrame(
        [
            {
                "Source airport": "JFK",
                "Destination airport": "LAX",
                "Airline (Normalized)": normalize_name("Sample Airways"),
                "Equipment": "738",
            }
        ]
    )

    enriched = datastore.process_routes(routes_df)
    assert enriched.loc[0, "Total"] == GENERIC_SEAT_GUESSES["738"]


def test_find_competing_routes_identifies_common_pairs(datastore):
    airline_x_df = pd.DataFrame(
        [
            {
                "Source airport": "JFK",
                "Destination airport": "LAX",
                "Airline (Normalized)": "sample",
                "ASM": 10000.0,
                "Equipment": "A320",
            }
        ]
    )
    airline_y_df = pd.DataFrame(
        [
            {
                "Source airport": "JFK",
                "Destination airport": "LAX",
                "Airline (Normalized)": "other",
                "ASM": 8000.0,
                "Equipment": "B738",
            }
        ]
    )

    competing = datastore.find_competing_routes(airline_x_df, airline_y_df)

    assert len(competing) == 1
    assert competing.loc[0, "Source"] == "JFK"
    assert competing.loc[0, "Dest"] == "LAX"
    assert competing.loc[0, "sample ASM"] == "10,000"
    assert competing.loc[0, "other ASM"] == "8,000"
    assert competing.loc[0, "sample Aircraft"] == "A320"
    assert competing.loc[0, "other Aircraft"] == "B738"
