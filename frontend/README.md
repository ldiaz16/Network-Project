# Airline Route Optimizer UI

This folder contains a lightweight browser UI that talks to the FastAPI backend in `src/api.py`.

## Prerequisites

1. Install the backend dependencies (these are not pinned, adjust as needed):

   ```bash
   pip install fastapi uvicorn pydantic
   ```

2. Ensure the dataset files (`data/airlines.dat`, `data/routes.dat`, `data/airports.dat`, `data/cbsa.csv`) are present. The existing `Makefile` target `data` can download them.

## Run the backend

Start the FastAPI app (defaults to `http://localhost:8000`):

```bash
uvicorn src.api:app --reload
```

You can also run `make api`, which uses the same command.

## Serve the frontend

Any static file server works. For example:

```bash
python -m http.server 5173 --directory frontend
```

Then open `http://localhost:5173` in your browser. The UI talks to the backend at `http://localhost:8000/api`.

## Using the UI

1. (Optional) Enter two airlines for comparison and leave “Skip comparison” unchecked. Autocomplete suggestions appear as you type.
2. Provide additional airlines for CBSA simulation (one per line) if you only need CBSA results.
3. Adjust CBSA parameters or trigger a cache build if needed.
4. Click **Run Analysis**. Results appear in the lower panel—network summaries, competing routes, and CBSA suggestions.

API errors or validation issues are surfaced above the results section. If a CBSA lookup fails for a specific airline, the UI skips it and shows a note in the “messages” area.
