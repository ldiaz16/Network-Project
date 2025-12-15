# Makefile for airline_route_optimizer

.PHONY: all run data report journal clean

# Default: run everything
all: data report run

# Step 1: Install Python dependencies

# Step 2: Ensure data files exist (BTS T-100 + lookup tables)
data:
	python3 scripts/ensure_data.py

# Step 3: Run the main script
run:
	python3 main.py

api:
	uvicorn src.api:app --reload

report: data
	python3 scripts/report.py --output-markdown reports/demo_report.md --output-pdf reports/demo_report.pdf

asm-dashboard:
	python3 scripts/asm_dashboard.py --input reports/asm_summary_history.csv --output reports/asm_dashboard.md

asm-check:
	python3 scripts/check_asm_quality.py --input reports/asm_summary_history.csv --fail-on-alert

# Optional: create a journal entry for today
journal:
	python3 scripts/create_journal.py

# Clean logs or cache files (expand as needed)
clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
