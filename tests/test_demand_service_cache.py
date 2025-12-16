from pathlib import Path

import pandas as pd

from src.demand_service import DemandMartCache


def _write_mart(path: Path) -> None:
    df = pd.DataFrame(
        [
            {"year": 2022, "quarter": 1, "origin": "JFK", "dest": "LAX", "passengers": 10.0, "avg_fare": 200.0, "distance": 2500.0},
            {"year": 2022, "quarter": 1, "origin": "LAX", "dest": "JFK", "passengers": 5.0, "avg_fare": 180.0, "distance": 2500.0},
            {"year": 2023, "quarter": 1, "origin": "MCO", "dest": "PHL", "passengers": 7.0, "avg_fare": 150.0, "distance": 900.0},
        ]
    )
    df.to_csv(path, index=False)


def test_market_totals_cache_key_uses_mtime(tmp_path: Path):
    mart_path = tmp_path / "mart.csv"
    _write_mart(mart_path)

    cache = DemandMartCache(base_dir=tmp_path, mart_path=mart_path, max_cache_entries=10)
    totals_first = cache.get_market_totals(since_year=2022, directional=False, mart_path=mart_path)

    assert cache.mart_mtime_ns != 0
    expected_key = (2022, False, cache.mart_mtime_ns)
    assert expected_key in cache.market_totals

    totals_second = cache.get_market_totals(since_year=2022, directional=False, mart_path=mart_path)
    assert totals_first is totals_second


def test_market_totals_prunes_cache_entries(tmp_path: Path):
    mart_path = tmp_path / "mart.csv"
    _write_mart(mart_path)

    cache = DemandMartCache(base_dir=tmp_path, mart_path=mart_path, max_cache_entries=1)
    cache.get_market_totals(since_year=2022, directional=False, mart_path=mart_path)
    cache.get_market_totals(since_year=2023, directional=False, mart_path=mart_path)

    assert len(cache.market_totals) == 1

