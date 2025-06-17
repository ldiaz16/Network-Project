# Makefile for airline_route_optimizer

.PHONY: all run data journal clean

# Default: run everything
all: data run

# Step 1: Install Python dependencies

# Step 2: Ensure data files exist (airlines, routes, airports)
data:
	@mkdir -p data
	@python3 -c "import os; f='data/airlines.dat'; print(f'Data check: {f}') if os.path.exists(f) else __import__('urllib.request').urlretrieve('https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat', f)"
	@python3 -c "import os; f='data/routes.dat'; print(f'Data check: {f}') if os.path.exists(f) else __import__('urllib.request').urlretrieve('https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat', f)"
	@python3 -c "import os; f='data/airports.dat'; print(f'Data check: {f}') if os.path.exists(f) else __import__('urllib.request').urlretrieve('https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat', f)"

# Step 3: Run the main script
run:
	python3 main.py

# Optional: create a journal entry for today
journal:
	python3 scripts/create_journal.py

# Clean logs or cache files (expand as needed)
clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
