from src.backend_service import get_airline_fleet_profile
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
