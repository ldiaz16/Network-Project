import pandas as pd
import pytest
from geopy.distance import geodesic
from types import MethodType

from data.airlines import normalize_name
from src.load_data import DataStore, GENERIC_SEAT_GUESSES
from tests.helpers import seed_sample_airline, seed_competing_routes


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
    metrics = datastore.analyze_network(graph, processed_routes=routes_df)

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
    datastore.routes = pd.DataFrame(
        [
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "JFK",
                "Source airport ID": None,
                "Destination airport": "LAX",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A320",
            },
            {
                "Airline Code": "OA",
                "IDK": None,
                "Source airport": "JFK",
                "Source airport ID": None,
                "Destination airport": "LAX",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A320",
            },
        ]
    )

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


def test_analyze_route_market_share_returns_competition(datastore):
    seed_competing_routes(datastore)

    result = datastore.analyze_route_market_share([("AAA", "BBB"), ("AAA", "DDD")], top_airlines=1)

    assert "routes" in result
    assert len(result["routes"]) == 2
    overlap = result["routes"][0]
    assert overlap["source"] == "AAA" and overlap["destination"] == "BBB"
    assert overlap["status"] == "ok"
    assert overlap["airline_count"] == 2
    # Only the top airline should be returned due to the limit of 1.
    assert len(overlap["airlines"]) == 1
    assert overlap["airlines"][0]["market_share"] > 0
    solo = result["routes"][1]
    assert solo["destination"] == "DDD"
    assert len(solo["airlines"]) == 1
    assert solo["airlines"][0]["market_share"] == pytest.approx(1.0)


def test_analyze_route_market_share_validates_routes(datastore):
    seed_competing_routes(datastore)

    with pytest.raises(ValueError):
        datastore.analyze_route_market_share([], top_airlines=3)


def test_auto_base_airports_from_route_share(datastore):
    routes = (
        [{"Source airport": "HUB", "Destination airport": "A"} for _ in range(25)]
        + [{"Source airport": "FOC", "Destination airport": "B"} for _ in range(8)]
        + [{"Source airport": "OFF", "Destination airport": "C"} for _ in range(1)]
    )
    routes_df = pd.DataFrame(routes)

    bases = datastore._derive_base_airports(routes_df, "Sample Air")

    assert "HUB" in bases["hubs"]
    assert "FOC" in bases["focus_cities"]
    assert "OFF" in bases["off_points"]
    assert bases["source"].startswith("auto")


def test_manual_bases_override_hub_scoring(datastore):
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "JFK", "Name": "John F. Kennedy", "Latitude": 40.6413, "Longitude": -73.7781},
            {"IATA": "BGM", "Name": "Greater Binghamton", "Latitude": 42.2087, "Longitude": -75.9796},
        ]
    )
    datastore.routes = pd.DataFrame(
        [
            {
                "Airline Code": "AA",
                "IDK": None,
                "Source airport": "DFW",
                "Source airport ID": None,
                "Destination airport": "LAX",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "738",
            }
        ]
    )
    datastore.airlines = pd.DataFrame(
        [
            {"Airline": "American Airlines", "Alias": "", "IATA": "AA", "ICAO": "", "Callsign": "", "Country": "US", "Active": "Y"}
        ]
    )
    datastore.airlines["Airline (Normalized)"] = datastore.airlines["Airline"].apply(normalize_name)
    datastore.aircraft_config = pd.DataFrame(columns=["Airline", "Aircraft", "Y", "W", "J", "F", "Total"])
    datastore.equipment_capacity_lookup = datastore._build_equipment_capacity_lookup(datastore.aircraft_config)
    datastore.airline_bases = {
        normalize_name("American Airlines"): {
            "hubs": ["JFK"],
            "focus_cities": [],
            "off_points": [],
            "source": "manual",
        }
    }

    result = datastore.evaluate_route_opportunity("American Airlines", "JFK", "BGM")

    assert result["hub_fit_label"].startswith("Hub-to-")
    assert result["hub_fit_source"] == "manual"


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


def test_strategy_baseline_uses_global_asm_signal(datastore):
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "JFK", "Name": "John F. Kennedy", "Latitude": 40.6413, "Longitude": -73.7781},
            {"IATA": "LAX", "Name": "Los Angeles", "Latitude": 33.9416, "Longitude": -118.4085},
            {"IATA": "ORD", "Name": "O'Hare", "Latitude": 41.9742, "Longitude": -87.9073},
        ]
    )
    config = {
        "Sample Airways": {
            "A320": {"Y": 150, "W": 10, "J": 12, "F": 4, "Total": 176},
        }
    }
    datastore.aircraft_config = datastore.convert_aircraft_config_to_df(config)
    route_columns = [
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
            {"Airline Code": "SA", "Source airport": "JFK", "Destination airport": "LAX", "Equipment": "A320"},
            {"Airline Code": "SA", "Source airport": "JFK", "Destination airport": "ORD", "Equipment": "A320"},
            {"Airline Code": "OA", "Source airport": "JFK", "Destination airport": "LAX", "Equipment": "A320"},
        ],
        columns=route_columns,
    )

    routes_df = pd.DataFrame(
        [
            {
                "Source airport": "JFK",
                "Destination airport": "LAX",
                "Airline (Normalized)": normalize_name("Sample Airways"),
                "Equipment": "A320",
            },
            {
                "Source airport": "JFK",
                "Destination airport": "ORD",
                "Airline (Normalized)": normalize_name("Sample Airways"),
                "Equipment": "A320",
            },
        ]
    )

    enriched = datastore.process_routes(routes_df)

    def fake_totals(self, route_pairs):
        mapping = {
            ("JFK", "LAX"): 500000.0,
            ("JFK", "ORD"): 2000000.0,
        }
        return {pair: mapping.get(pair, 1000000.0) for pair in route_pairs}

    datastore.get_route_total_asm = MethodType(fake_totals, datastore)

    analyzed = datastore.cost_analysis(enriched)
    baseline_lookup = analyzed.set_index(["Source airport", "Destination airport"])["Route Strategy Baseline"].to_dict()

    assert baseline_lookup[("JFK", "LAX")] > baseline_lookup[("JFK", "ORD")]
    assert all(0 <= value <= 1 for value in baseline_lookup.values())


def test_build_route_scorecard_returns_profiles(datastore):
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
    datastore.routes = pd.DataFrame(
        [
            {"Airline Code": "SA", "Source airport": "JFK", "Destination airport": "LAX", "Equipment": "A320"},
        ]
    )
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
    cost_df = datastore.cost_analysis(datastore.process_routes(routes_df))
    scorecard = datastore.build_route_scorecard(cost_df)
    assert "competition" in scorecard
    assert "maturity" in scorecard
    assert "yield" in scorecard
    assert scorecard["competition"]


def test_market_share_snapshot_uses_total_asm(datastore):
    sample = pd.DataFrame(
        [
            {
                "Source airport": "A",
                "Destination airport": "B",
                "ASM": 1000.0,
                "Competition Level": "Monopoly",
            },
            {
                "Source airport": "A",
                "Destination airport": "C",
                "ASM": 500.0,
                "Competition Level": "Duopoly",
            },
        ]
    )

    def fake_totals(self, pairs):
        mapping = {("A", "B"): 2000.0, ("A", "C"): 500.0}
        return {pair: mapping.get(pair, 0.0) for pair in pairs}

    datastore.get_route_total_asm = MethodType(fake_totals, datastore)
    snapshot = datastore.compute_market_share_snapshot(sample, limit=2)
    ab_share = snapshot.loc[snapshot["Destination"] == "B", "Market Share"].iloc[0]
    ac_share = snapshot.loc[snapshot["Destination"] == "C", "Market Share"].iloc[0]
    assert ab_share == pytest.approx(0.5)
    assert ac_share == pytest.approx(1.0)


def test_summarize_fleet_utilization_returns_scores(datastore):
    sample = pd.DataFrame(
        [
            {
                "Source airport": "A",
                "Destination airport": "B",
                "Equipment": "A320",
                "Distance (miles)": 1000,
                "Total Seats": 150,
            },
            {
                "Source airport": "A",
                "Destination airport": "C",
                "Equipment": "A320",
                "Distance (miles)": 900,
                "Total Seats": 160,
            },
            {
                "Source airport": "D",
                "Destination airport": "E",
                "Equipment": "737",
                "Distance (miles)": 700,
                "Total Seats": 140,
            },
        ]
    )
    util = datastore.summarize_fleet_utilization(sample)
    assert set(util.columns) == {
        "Equipment",
        "Route Count",
        "Average Distance",
        "Total Distance",
        "Utilization Score",
    }
    assert util.iloc[0]["Equipment"] == "A320"
    assert util.iloc[0]["Route Count"] == 2
    total_asm = 150000 + 144000 + 98000
    asm_share = (150000 + 144000) / total_asm
    route_share = 2 / 3  # two routes for A320 out of three total
    expected_score = round(0.6 * asm_share + 0.4 * route_share, 3)
    assert util.iloc[0]["Utilization Score"] == pytest.approx(expected_score, rel=1e-6)


def test_summarize_fleet_utilization_splits_multi_equipment_entries(datastore):
    sample = pd.DataFrame(
        [
            {
                "Source airport": "A",
                "Destination airport": "B",
                "Equipment": "738 M88",
                "Distance (miles)": 1000,
                "Total Seats": 150,
            },
            {
                "Source airport": "A",
                "Destination airport": "C",
                "Equipment": "738",
                "Distance (miles)": 500,
                "Total Seats": 160,
            },
        ]
    )
    util = datastore.summarize_fleet_utilization(sample)
    records = {row["Equipment"]: row for _, row in util.iterrows()}
    assert "738" in records and "M88" in records
    assert records["738"]["Route Count"] == 2
    assert records["M88"]["Route Count"] == 1
    asm_route1 = 150 * 1000
    asm_route2 = 160 * 500
    total_asm = asm_route1 + asm_route2
    asm_share_m88 = (asm_route1 / 2) / total_asm
    route_share_m88 = 1 / 3  # one route for M88 out of three equipment-route combos
    expected_m88_score = 0.6 * asm_share_m88 + 0.4 * route_share_m88
    assert records["738"]["Utilization Score"] > records["M88"]["Utilization Score"]
    assert records["M88"]["Utilization Score"] == pytest.approx(round(expected_m88_score, 3), rel=1e-6)


def test_find_best_aircraft_for_route_ranks_by_distance_and_seats(datastore):
    seed_sample_airline(datastore)

    recommendations = datastore.find_best_aircraft_for_route(
        "Sample Airways",
        route_distance=650,
        seat_demand=170,
        top_n=2
    )

    assert list(recommendations["Equipment"]) == ["A320", "A321"]
    assert recommendations.iloc[0]["Seat Capacity"] == 170


def test_load_factor_metrics_adjust_scoring(datastore):
    seed_sample_airline(datastore)
    normalized = normalize_name("Sample Airways")

    datastore.operational_metrics = {normalized: {"load_factor": 0.9}}
    high_pressure = datastore.find_best_aircraft_for_route(
        "Sample Airways",
        route_distance=650,
        seat_demand=170,
        top_n=2,
    ).set_index("Equipment")

    datastore.operational_metrics = {normalized: {"load_factor": 0.78}}
    low_pressure = datastore.find_best_aircraft_for_route(
        "Sample Airways",
        route_distance=650,
        seat_demand=170,
        top_n=2,
    ).set_index("Equipment")

    assert high_pressure.loc["A320", "Airline Load Factor"] == pytest.approx(0.9)
    assert low_pressure.loc["A320", "Airline Load Factor"] == pytest.approx(0.78)
    assert high_pressure.loc["A320", "Load Factor Pressure"] > low_pressure.loc["A320", "Load Factor Pressure"]
    assert high_pressure.loc["A320", "Optimal Score"] > low_pressure.loc["A320", "Optimal Score"]


def test_find_best_aircraft_filters_to_airline_fleet(datastore):
    seed_sample_airline(datastore)
    # Inject an out-of-fleet equipment into routes; config does not include it.
    datastore.routes = pd.concat(
        [
            datastore.routes,
            pd.DataFrame(
                [
                    {
                        "Airline Code": "SA",
                        "IDK": None,
                        "Source airport": "AAA",
                        "Source airport ID": None,
                        "Destination airport": "EEE",
                        "Destination airport ID": None,
                        "Codeshare": None,
                        "Stops": 0,
                        "Equipment": "A346",
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    datastore.airports = pd.concat(
        [
            datastore.airports,
            pd.DataFrame([{"IATA": "EEE", "Name": "Airport E", "Latitude": 25.0, "Longitude": 35.0}]),
        ],
        ignore_index=True,
    )

    recommendations = datastore.find_best_aircraft_for_route(
        "Sample Airways",
        route_distance=2000,
        seat_demand=250,
        top_n=5,
    )

    assert "A346" not in recommendations["Equipment"].tolist()


def test_cbsa_simulation_filters_international_and_rounds(datastore):
    datastore.airports = pd.DataFrame(
        [
            {"IATA": "AAA", "Name": "Airport A", "City": "Alpha", "Country": "United States", "Latitude": 35.0, "Longitude": -80.0},
            {"IATA": "BBB", "Name": "Airport B", "City": "Beta", "Country": "United States", "Latitude": 36.0, "Longitude": -81.0},
            {"IATA": "CCC", "Name": "Airport C", "City": "Gamma", "Country": "Canada", "Latitude": 45.0, "Longitude": -75.0},
        ]
    )

    cbsa_meta = {
        "AAA": {"CBSA Name": "Metro AAA", "CBSA Code": "1001", "County/County Equivalent": "Alpha County", "State Name": "State A"},
        "BBB": {"CBSA Name": "Metro BBB", "CBSA Code": "1002", "County/County Equivalent": "Beta County", "State Name": "State B"},
        "CCC": {"CBSA Name": "International", "CBSA Code": "9999", "County/County Equivalent": "Gamma County", "State Name": "Province C"},
    }

    def fake_annotate(self, airports_df):
        metadata = []
        for _, row in airports_df.iterrows():
            meta = cbsa_meta.get(row["IATA"], {})
            metadata.append(
                {
                    "County/County Equivalent": meta.get("County/County Equivalent"),
                    "State Name": meta.get("State Name"),
                    "CBSA Name": meta.get("CBSA Name", ""),
                    "CBSA Code": meta.get("CBSA Code"),
                }
            )
        return pd.concat([airports_df.reset_index(drop=True), pd.DataFrame(metadata)], axis=1)

    datastore.annotate_airports_with_cbsa = MethodType(fake_annotate, datastore)

    airline_cost_df = pd.DataFrame(
        [
            {
                "Source airport": "AAA",
                "Destination airport": "BBB",
                "ASM": 1234.56789,
                "Total Seats": 150.0,
                "Distance (miles)": 500.0,
                "Seats per Mile": 0.3,
            },
            {
                "Source airport": "AAA",
                "Destination airport": "CCC",
                "ASM": 9876.54321,
                "Total Seats": 160.0,
                "Distance (miles)": 600.0,
                "Seats per Mile": 0.27,
            },
        ]
    )

    simulation = datastore.simulate_cbsa_route_opportunities(airline_cost_df, top_n=5, max_suggestions_per_route=1)
    best_routes = simulation["best_routes"]
    assert list(best_routes["Route"]) == ["AAA->BBB"]
    assert "Source CBSA" not in best_routes.columns
    assert "Destination CBSA" not in best_routes.columns
    assert "ASM Share" in best_routes.columns
    assert best_routes.loc[0, "ASM Share"] == "100.0%"
    assert "Route Rationale" in best_routes.columns
    assert simulation["suggested_routes"].empty


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
    assert "sample ASM" not in competing.columns
    assert "other ASM" not in competing.columns
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


def test_select_airline_routes_respects_codeshare_overrides(datastore):
    seed_sample_airline(datastore)
    extra = pd.DataFrame(
        [
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "ZZZ",
                "Source airport ID": None,
                "Destination airport": "AAA",
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
                "Destination airport": "EEE",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A321",
            },
            {
                "Airline Code": "SA",
                "IDK": None,
                "Source airport": "EEE",
                "Source airport ID": None,
                "Destination airport": "AAA",
                "Destination airport ID": None,
                "Codeshare": None,
                "Stops": 0,
                "Equipment": "A321",
            },
        ]
    )
    datastore.routes = pd.concat([datastore.routes, extra], ignore_index=True)
    normalized = normalize_name("Sample Airways")
    datastore.codeshare_overrides = {
        normalized: {
            "blocked_airports": {"ZZZ"},
            "blocked_pairs": {"AAA||EEE"},
        }
    }

    routes_df, _ = datastore.select_airline_routes("Sample Airways")

    assert not routes_df["Source airport"].eq("ZZZ").any()
    assert not routes_df["Destination airport"].eq("ZZZ").any()
    blocked = ((routes_df["Source airport"] == "AAA") & (routes_df["Destination airport"] == "EEE")) | (
        (routes_df["Source airport"] == "EEE") & (routes_df["Destination airport"] == "AAA")
    )
    assert not blocked.any()


def test_select_airline_routes_accepts_iata_code_queries(datastore):
    seed_sample_airline(datastore)

    routes_df, meta = datastore.select_airline_routes("SA")

    assert meta["IATA"] == "SA"
    assert not routes_df.empty
    assert routes_df["Airline Code"].str.upper().eq("SA").all()
    assert routes_df["Airline"].eq("Sample Airways").all()


def test_select_airline_routes_accepts_parenthetical_code_queries(datastore):
    seed_sample_airline(datastore)

    routes_df, meta = datastore.select_airline_routes("Sample Airways (SA)")

    assert meta["IATA"] == "SA"
    assert not routes_df.empty
    assert routes_df["Airline Code"].str.upper().eq("SA").all()
    assert routes_df["Airline"].eq("Sample Airways").all()


def test_select_airline_routes_accepts_icao_code_queries(datastore):
    seed_sample_airline(datastore)

    routes_df, meta = datastore.select_airline_routes("SMP")

    assert meta["IATA"] == "SA"
    assert meta["ICAO"] == "SMP"
    assert not routes_df.empty
    assert routes_df["Airline Code"].str.upper().eq("SA").all()
    assert routes_df["Airline"].eq("Sample Airways").all()
