# Airline Route Optimizer Report

_Auto-generated using scripts/report.py._

**Airlines showcased:** Delta Air Lines, American Airlines

## Network Snapshots

### Delta Air Lines

* Routes analyzed: 1146
* Airports served: 230
* Total routes: 1146
* Top hubs: [('ATL', 337), ('JFK', 153), ('DTW', 153), ('MSP', 120), ('SLC', 86)]

### American Airlines

* Routes analyzed: 1265
* Airports served: 259
* Total routes: 1265
* Top hubs: [('DFW', 224), ('CLT', 219), ('MIA', 190), ('PHL', 185), ('PHX', 112)]

## ASM Accuracy Snapshot

### Delta Air Lines

| Seat Source | Routes | Valid ASM Routes | Total Seats | Total ASM | ASM Share |
| --- | --- | --- | --- | --- | --- |
| airline_config | 732 | 732 | 120663 | 226436066 | 81.6% |
| equipment_estimate | 307 | 307 | 41472 | 51095635 | 18.4% |
| unknown | 107 | 0 | 0 | 0 | 0.0% |


_ASM data quality looks healthy._

### American Airlines

| Seat Source | Routes | Valid ASM Routes | Total Seats | Total ASM | ASM Share |
| --- | --- | --- | --- | --- | --- |
| airline_config | 1180 | 1180 | 198094 | 394852642 | 91.1% |
| equipment_estimate | 65 | 65 | 9900 | 38787007 | 8.9% |
| unknown | 20 | 0 | 0 | 0 | 0.0% |


_ASM data quality looks healthy._

## Competing Route Overlap


| Source | Dest |
| --- | --- |
| ATL | DFW |
| ATL | MIA |
| AUS | JFK |
| BCN | JFK |
| BDA | JFK |
| BKK | NRT |
| BNA | LAX |
| BOS | CUN |
| BOS | JFK |
| BOS | LAX |


## CBSA Opportunity Highlights

### Delta Air Lines
- Top corridor **HNL-ATL** links — (ASM 1.27M, score 0.71).
- 5 total CBSA corridors met the performance filter.
- No CBSA-similar opportunities surfaced.

**Top CBSA corridors**
| Rank | Route | CBSA Pair | ASM | Total Seats | Performance Score |
| --- | --- | --- | --- | --- | --- |
| 1 | HNL-ATL | — | 1.27M | 281 | 0.71 |
| 2 | ATL-HNL | — | 1.27M | 281 | 0.71 |
| 3 | HNL-SLC | — | 677K | 226 | 0.39 |
| 4 | SLC-HNL | — | 677K | 226 | 0.39 |
| 5 | SEA-HNL | — | 626K | 234 | 0.37 |

**Suggested CBSA opportunities**
_No CBSA-similar opportunities surfaced._


### American Airlines
- Top corridor **DFW-HNL** links — (ASM 904K, score 0.71).
- 5 total CBSA corridors met the performance filter.
- Best opportunity **FLL-LAX** mirrors Miami-Fort Lauderdale-West Palm Beach, FL <-> Los Angeles-Long Beach-Anaheim, CA (100% distance match vs MIA-LAX, score 0.62).

**Top CBSA corridors**
| Rank | Route | CBSA Pair | ASM | Total Seats | Performance Score |
| --- | --- | --- | --- | --- | --- |
| 1 | DFW-HNL | — | 904K | 239 | 0.71 |
| 2 | HNL-DFW | — | 904K | 239 | 0.71 |
| 3 | OGG-DFW | — | 887K | 239 | 0.70 |
| 4 | DFW-OGG | — | 887K | 239 | 0.70 |
| 5 | MIA-LAX | — | 770K | 329 | 0.62 |

**Suggested CBSA opportunities**
| Rank | Proposed Route | CBSA Pair | Reference Route | Distance Match | Opportunity Score |
| --- | --- | --- | --- | --- | --- |
| 1 | FLL-LAX | Miami-Fort Lauderdale-West Palm Beach, FL <-> Los Angeles-Long Beach-Anaheim, CA | MIA-LAX | 100% | 0.62 |
| 2 | FLL-LGB | Miami-Fort Lauderdale-West Palm Beach, FL <-> Los Angeles-Long Beach-Anaheim, CA | MIA-LAX | 99% | 0.62 |
| 3 | FLL-SNA | Miami-Fort Lauderdale-West Palm Beach, FL <-> Los Angeles-Long Beach-Anaheim, CA | MIA-LAX | 99% | 0.62 |


## Next Steps

* Run `python3 main.py --airline AIRLINE1 --airline AIRLINE2` for interactive comparisons.
* Seed CBSA data via `python3 main.py --build-cbsa-cache`.
* Use `scripts/showcase.py` for a quick CLI preview.
