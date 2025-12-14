"""Airline helpers shared across the project.

The app no longer downloads OpenFlights data at import time. Data acquisition is handled
via `Makefile`/scripts, and the runtime loaders in `src/load_data.py` prefer BTS T-100
and related BTS lookup tables.
"""

def normalize_name(name):
    if not isinstance(name, str):
        return ""
    lowered = name.lower()
    for token in ["airlines", "airways", "air line", "airline", "air"]:
        lowered = lowered.replace(token, " ")
    return "".join(ch for ch in lowered if ch.isalnum()).strip()
