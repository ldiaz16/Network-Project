"""
Deprecated download helper.

This project now sources airport names/codes from BTS lookup tables in `Lookup Tables/`.
If you need airport coordinates (for CBSA lookups or geodesic distances), provide a local
OurAirports CSV at `data/airports.csv` (with `iata_code`, `latitude_deg`, `longitude_deg`, etc.).
"""

if __name__ == "__main__":
    raise SystemExit(
        "Deprecated: OpenFlights airport downloads are no longer used. "
        "Provide BTS T-100 routes + lookup tables instead."
    )
