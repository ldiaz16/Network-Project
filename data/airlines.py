import os
import urllib.request

# Ensure target directory
DATA_DIR = "data"
AIRLINES_PATH = os.path.join(DATA_DIR, "airlines.dat")

# Only download if file does not exist
if not os.path.exists(AIRLINES_PATH):
    print(f"Downloading airlines.dat to {AIRLINES_PATH}...")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat",
        AIRLINES_PATH
    )
else:
    print(f"airlines.dat already exists at {AIRLINES_PATH}")

def normalize_name(name):
    name = name.lower()
    for word in ["airlines", "airways", "air"]:
        name = name.replace(word, "")
    return ''.join(filter(str.isalnum, name)).strip()
