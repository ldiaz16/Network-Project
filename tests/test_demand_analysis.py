import pandas as pd

from src.demand_analysis import (
    BIG3_AIRPORTS,
    aggregate_market_totals,
    build_market_quarterly,
    compute_concentration,
    market_stability_analysis,
    rank_markets,
)


def test_build_market_quarterly_combines_directions_when_undirected():
    mart = pd.DataFrame(
        [
            {"year": 2022, "quarter": 1, "origin": "JFK", "dest": "LAX", "passengers": 10.0, "avg_fare": 200.0, "distance": 2500.0},
            {"year": 2022, "quarter": 1, "origin": "LAX", "dest": "JFK", "passengers": 5.0, "avg_fare": 180.0, "distance": 2500.0},
        ]
    )

    out = build_market_quarterly(mart, directional=False, since_year=2022)

    assert len(out) == 1
    row = out.iloc[0]
    assert row["market_a"] == "JFK"
    assert row["market_b"] == "LAX"
    assert row["passengers"] == 15.0
    assert row["avg_fare"] == (10 * 200 + 5 * 180) / 15
    assert row["distance"] == 2500.0
    assert row["fare_per_mile"] == row["avg_fare"] / row["distance"]
    assert row["market"] == "JFK-LAX"


def test_build_market_quarterly_keeps_directional_markets_separate():
    mart = pd.DataFrame(
        [
            {"year": 2022, "quarter": 1, "origin": "JFK", "dest": "LAX", "passengers": 10.0, "avg_fare": 200.0, "distance": 2500.0},
            {"year": 2022, "quarter": 1, "origin": "LAX", "dest": "JFK", "passengers": 5.0, "avg_fare": 180.0, "distance": 2500.0},
        ]
    )

    out = build_market_quarterly(mart, directional=True, since_year=2022)

    assert len(out) == 2
    assert set(out["market"]) == {"JFK-LAX", "LAX-JFK"}


def test_aggregate_market_totals_weights_fare_and_distance_by_passengers():
    mart = pd.DataFrame(
        [
            {"year": 2022, "quarter": 1, "origin": "JFK", "dest": "LAX", "passengers": 10.0, "avg_fare": 200.0, "distance": 2500.0},
            {"year": 2022, "quarter": 2, "origin": "JFK", "dest": "LAX", "passengers": 5.0, "avg_fare": 220.0, "distance": 2500.0},
        ]
    )
    quarterly = build_market_quarterly(mart, directional=False, since_year=2022)
    totals = aggregate_market_totals(quarterly)

    assert len(totals) == 1
    row = totals.iloc[0]
    assert row["passengers_total"] == 15.0
    expected_avg_fare = (10 * 200 + 5 * 220) / 15
    assert row["avg_fare"] == expected_avg_fare
    assert row["distance"] == 2500.0
    assert row["fare_per_mile"] == expected_avg_fare / 2500.0


def test_rank_markets_excludes_big3_endpoints():
    totals = pd.DataFrame(
        [
            {"market_a": "JFK", "market_b": "LAX", "passengers_total": 1000.0, "market": "JFK-LAX"},
            {"market_a": "MCO", "market_b": "PHL", "passengers_total": 900.0, "market": "MCO-PHL"},
        ]
    )
    ranked = rank_markets(totals, top_n=50, exclude_airports=BIG3_AIRPORTS)
    assert len(ranked) == 1
    assert ranked.iloc[0]["market"] == "MCO-PHL"


def test_compute_concentration_top_decile_share():
    totals = pd.DataFrame(
        [
            {"passengers_total": 90.0},
            {"passengers_total": 5.0},
            {"passengers_total": 5.0},
        ]
    )
    stats = compute_concentration(totals, top_share=0.10)
    assert stats.markets == 3
    assert stats.top_markets == 1
    assert stats.total_passengers == 100.0
    assert stats.top_passengers == 90.0
    assert stats.top_passenger_share == 0.9


def test_market_stability_classifies_stable_seasonal_and_volatile():
    market_quarterly = pd.DataFrame(
        [
            # Stable: constant across quarters
            {"year": 2022, "quarter": 1, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            {"year": 2022, "quarter": 2, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            {"year": 2022, "quarter": 3, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            {"year": 2022, "quarter": 4, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            {"year": 2023, "quarter": 1, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            {"year": 2023, "quarter": 2, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            {"year": 2023, "quarter": 3, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            {"year": 2023, "quarter": 4, "market_a": "AAA", "market_b": "BBB", "passengers": 100.0},
            # Seasonal: perfectly repeats within the available year
            {"year": 2022, "quarter": 1, "market_a": "CCC", "market_b": "DDD", "passengers": 50.0},
            {"year": 2022, "quarter": 2, "market_a": "CCC", "market_b": "DDD", "passengers": 150.0},
            {"year": 2022, "quarter": 3, "market_a": "CCC", "market_b": "DDD", "passengers": 50.0},
            {"year": 2022, "quarter": 4, "market_a": "CCC", "market_b": "DDD", "passengers": 150.0},
            {"year": 2023, "quarter": 1, "market_a": "CCC", "market_b": "DDD", "passengers": 50.0},
            {"year": 2023, "quarter": 2, "market_a": "CCC", "market_b": "DDD", "passengers": 150.0},
            {"year": 2023, "quarter": 3, "market_a": "CCC", "market_b": "DDD", "passengers": 50.0},
            {"year": 2023, "quarter": 4, "market_a": "CCC", "market_b": "DDD", "passengers": 150.0},
            # Volatile: same quarters swing year-to-year (residual volatility remains after seasonal means)
            {"year": 2022, "quarter": 1, "market_a": "EEE", "market_b": "FFF", "passengers": 100.0},
            {"year": 2022, "quarter": 2, "market_a": "EEE", "market_b": "FFF", "passengers": 100.0},
            {"year": 2022, "quarter": 3, "market_a": "EEE", "market_b": "FFF", "passengers": 100.0},
            {"year": 2022, "quarter": 4, "market_a": "EEE", "market_b": "FFF", "passengers": 100.0},
            {"year": 2023, "quarter": 1, "market_a": "EEE", "market_b": "FFF", "passengers": 0.0},
            {"year": 2023, "quarter": 2, "market_a": "EEE", "market_b": "FFF", "passengers": 200.0},
            {"year": 2023, "quarter": 3, "market_a": "EEE", "market_b": "FFF", "passengers": 0.0},
            {"year": 2023, "quarter": 4, "market_a": "EEE", "market_b": "FFF", "passengers": 200.0},
        ]
    )

    stability = market_stability_analysis(market_quarterly)
    by_market = {row["market"]: row["classification"] for _, row in stability.iterrows()}
    assert by_market["AAA-BBB"] == "Stable core"
    assert by_market["CCC-DDD"] == "Seasonal leisure"
    assert by_market["EEE-FFF"] == "Volatile / emerging"
