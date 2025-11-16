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
    assert enriched.loc[0, "Seat Source"] == "airline_config"


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
    assert enriched.loc[0, "Seat Source"] == "equipment_estimate"


def test_process_routes_infers_seats_from_config_tokens(datastore):
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "ATL", "Name": "Atlanta", "Latitude": 33.6407, "Longitude": -84.4277},
            {"IATA": "ORD", "Name": "Chicago", "Latitude": 41.9742, "Longitude": -87.9073},
        ]
    )
    config = {
        "Sample Airways": {
            "CRJ": {"Y": 50, "W": 0, "J": 0, "F": 0, "Total": 50},
            "CR7": {"Y": 65, "W": 0, "J": 0, "F": 0, "Total": 65},
        }
    }
    datastore.aircraft_config = datastore.convert_aircraft_config_to_df(config)

    routes_df = pd.DataFrame(
        [
            {
                "Source airport": "ATL",
                "Destination airport": "ORD",
                "Airline (Normalized)": normalize_name("Sample Airways"),
                "Equipment": "CRJ CR7",
            }
        ]
    )

    enriched = datastore.process_routes(routes_df)
    assert enriched.loc[0, "Total"] == 50
    assert enriched.loc[0, "Seat Source"] == "airline_config"


def test_summarize_asm_sources_groups_by_source(datastore):
    cost_df = pd.DataFrame(
        [
            {"Seat Source": "airline_config", "Total Seats": 200, "ASM": 1000, "ASM Valid": True},
            {"Seat Source": "airline_config", "Total Seats": 220, "ASM": 900, "ASM Valid": False},
            {"Seat Source": "equipment_estimate", "Total Seats": 150, "ASM": 500, "ASM Valid": True},
        ]
    )

    summary = datastore.summarize_asm_sources(cost_df)
    assert set(summary["Seat Source"]) == {"airline_config", "equipment_estimate"}
    config_row = summary[summary["Seat Source"] == "airline_config"].iloc[0]
    assert config_row["Routes"] == 2
    assert config_row["Valid ASM Routes"] == 1
    assert config_row["Total Seats"] == 420
    assert config_row["Total ASM"] == 1900
    assert config_row["ASM Share"] == "79.2%"
    assert pytest.approx(config_row["ASM Share Value"], rel=1e-3) == 0.7917


def test_detect_asm_alerts_flags_sources(datastore):
    cost_df = pd.DataFrame(
        [
            {"Seat Source": "equipment_estimate", "Total Seats": 200, "ASM": 800, "ASM Valid": True},
            {"Seat Source": "equipment_estimate", "Total Seats": 180, "ASM": 900, "ASM Valid": True},
            {"Seat Source": "unknown", "Total Seats": 150, "ASM": 700, "ASM Valid": False},
        ]
    )

    summary = datastore.summarize_asm_sources(cost_df)
    alerts = datastore.detect_asm_alerts(summary, estimate_threshold=0.4, unknown_threshold=0.1)
    assert any("equipment estimates" in alert for alert in alerts)
    assert any("lacks seat data" in alert for alert in alerts)


def test_detect_asm_alerts_combined_threshold(datastore):
    cost_df = pd.DataFrame(
        [
            {"Seat Source": "equipment_estimate", "Total Seats": 200, "ASM": 300, "ASM Valid": True},
            {"Seat Source": "unknown", "Total Seats": 180, "ASM": 220, "ASM Valid": True},
            {"Seat Source": "airline_config", "Total Seats": 620, "ASM": 480, "ASM Valid": True},
        ]
    )

    summary = datastore.summarize_asm_sources(cost_df)
    alerts = datastore.detect_asm_alerts(summary, estimate_threshold=0.5, unknown_threshold=0.5)
    assert alerts == ["52% of ASM is estimated or unknown; investigate data coverage."]


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
    assert competing.loc[0, "sample ASM"] == "10K"
    assert competing.loc[0, "other ASM"] == "8K"
    assert competing.loc[0, "sample ASM Share"] is None
    assert competing.loc[0, "other ASM Share"] is None
    assert competing.loc[0, "sample Aircraft"] == "A320"
    assert competing.loc[0, "other Aircraft"] == "B738"


def test_find_competing_routes_uses_all_airline_totals_for_share(datastore):
    airports = pd.DataFrame(
        [
            {"IATA": "SRC", "Name": "Source", "Latitude": 0.0, "Longitude": 0.0},
            {"IATA": "DST", "Name": "Destination", "Latitude": 0.0, "Longitude": 1.0},
        ]
    )
    airports["City"] = ""
    airports["Country"] = ""
    datastore.airports = airports

    airlines = pd.DataFrame(
        [
            {"Airline": "Sample Air", "Alias": "", "IATA": "SA", "ICAO": "", "Callsign": "", "Country": "US", "Active": "Y"},
            {"Airline": "Other Air", "Alias": "", "IATA": "OA", "ICAO": "", "Callsign": "", "Country": "US", "Active": "Y"},
            {"Airline": "Third Air", "Alias": "", "IATA": "TA", "ICAO": "", "Callsign": "", "Country": "US", "Active": "Y"},
        ]
    )
    airlines["Airline (Normalized)"] = airlines["Airline"].apply(normalize_name)
    datastore.airlines = airlines

    config = {
        "Sample Air": {"A320": {"Y": 100, "W": 0, "J": 0, "F": 0, "Total": 100}},
        "Other Air": {"A321": {"Y": 150, "W": 0, "J": 0, "F": 0, "Total": 150}},
        "Third Air": {"B738": {"Y": 200, "W": 0, "J": 0, "F": 0, "Total": 200}},
    }
    datastore.aircraft_config = datastore.convert_aircraft_config_to_df(config)

    routes_columns = [
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
            {"Airline Code": "SA", "Source airport": "SRC", "Destination airport": "DST", "Equipment": "A320"},
            {"Airline Code": "OA", "Source airport": "SRC", "Destination airport": "DST", "Equipment": "A321"},
            {"Airline Code": "TA", "Source airport": "SRC", "Destination airport": "DST", "Equipment": "B738"},
        ],
        columns=routes_columns,
    )

    sample_norm = normalize_name("Sample Air")
    other_norm = normalize_name("Other Air")
    distance = geodesic((0.0, 0.0), (0.0, 1.0)).miles

    airline_x_df = pd.DataFrame(
        [
            {
                "Source airport": "SRC",
                "Destination airport": "DST",
                "Airline (Normalized)": sample_norm,
                "ASM": distance * 100,
                "Equipment": "A320",
            }
        ]
    )
    airline_y_df = pd.DataFrame(
        [
            {
                "Source airport": "SRC",
                "Destination airport": "DST",
                "Airline (Normalized)": other_norm,
                "ASM": distance * 150,
                "Equipment": "A321",
            }
        ]
    )

    competing = datastore.find_competing_routes(airline_x_df, airline_y_df)

    assert competing.loc[0, "sample ASM Share"] == "22.2%"
    assert competing.loc[0, "other ASM Share"] == "33.3%"
