# Frontend

This directory hosts a lightweight static UI that talks to the FastAPI backend (`/api/`). The interface simply accepts an airline name/IATA code and displays:

- Summary counts (routes, seats, average/long routes).
- Network stats (hubs, airports, etc.).
- Top ASM-ranked routes pulled directly from `T_T100_SEGMENT_ALL_CARRIER.csv` (or `T_T100_SEGMENT_ALL_CARRIER.csv.gz`).
- An alliance/carrier-group view (`alliance.html`) powered by `/api/alliances` + `/api/alliance` (requires `T_T100_SEGMENT_ALL_CARRIER-2.csv` and `L_CARRIER_GROUP.csv`).
- A domestic demand markets view (`markets.html`) powered by `/api/demand/...` (requires the DB1B Domestic Demand Mart).

## Running

1. Make sure the backend is running (`uvicorn src.api:app --reload`).
2. Serve this folder statically, e.g.:

   ```bash
   python -m http.server 5173 --directory frontend
   ```

3. Visit http://localhost:5173 and enter an airline to run the route analysis.

No build step is required; the UI is vanilla JS + simple styling with no external frameworks.
