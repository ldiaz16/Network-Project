import pandas as pd

from src.data_utils import filter_codeshare_routes


def test_filter_codeshare_routes_drops_marketed_segments():
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
    data = [
        {"Airline Code": "UA", "Source airport": "IAH", "Destination airport": "LAX", "Codeshare": "", "Stops": 0, "Equipment": "320"},
        {"Airline Code": "UA", "Source airport": "IAH", "Destination airport": "AMS", "Codeshare": "Y", "Stops": 0, "Equipment": "330"},
        {"Airline Code": "UA", "Source airport": "IAH", "Destination airport": "FRA", "Codeshare": "y", "Stops": 0, "Equipment": "330"},
        {"Airline Code": "UA", "Source airport": "IAH", "Destination airport": "ORD", "Codeshare": "\\N", "Stops": 0, "Equipment": "738"},
    ]
    routes = pd.DataFrame(data, columns=columns)

    filtered = filter_codeshare_routes(routes)

    assert list(filtered["Destination airport"]) == ["LAX", "ORD"]
    assert not filtered["Codeshare"].str.upper().eq("Y").any()
