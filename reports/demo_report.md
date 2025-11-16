# Airline Route Optimizer Report

_Auto-generated using scripts/report.py._

**Airlines showcased:** Delta Air Lines, American Airlines

## Network Snapshots

### Delta Air Lines

* Routes analyzed: 1981
* Airports served: 354
* Total routes: 1981
* Top hubs: [('ATL', 419), ('DTW', 263), ('MSP', 254), ('SLC', 176), ('JFK', 172)]

### American Airlines

* Routes analyzed: 2354
* Airports served: 434
* Total routes: 2354
* Top hubs: [('DFW', 364), ('CLT', 267), ('ORD', 248), ('MIA', 240), ('PHL', 235)]

## ASM Accuracy Snapshot

### Delta Air Lines

| Seat Source | Routes | Valid ASM Routes | Total Seats | Total ASM | ASM Share |
| --- | --- | --- | --- | --- | --- |
| airline_config | 1361 | 1361 | 172026 | 296906186 | 69.0% |
| equipment_estimate | 459 | 459 | 63341 | 133411653 | 31.0% |
| unknown | 161 | 0 | 0 | 0 | 0.0% |


_ASM data quality looks healthy._

### American Airlines

| Seat Source | Routes | Valid ASM Routes | Total Seats | Total ASM | ASM Share |
| --- | --- | --- | --- | --- | --- |
| airline_config | 2106 | 2106 | 293785 | 558323011 | 91.7% |
| equipment_estimate | 170 | 170 | 18982 | 50636318 | 8.3% |
| unknown | 78 | 0 | 0 | 0 | 0.0% |


_ASM data quality looks healthy._

## Competing Route Overlap


| Source | Dest |
| --- | --- |
| ATL | CLT |
| ATL | DFW |
| ATL | LHR |
| ATL | MIA |
| ATL | ORD |
| ATL | PHL |
| ATL | PHX |
| AUS | JFK |
| BCN | JFK |
| BDA | JFK |


## CBSA Opportunity Highlights

### Delta Air Lines

**Top CBSA corridors**

| Route | Source CBSA Name | Destination CBSA Name | ASM | Total Seats | Performance Score |
| --- | --- | --- | --- | --- | --- |
| MEL-LAX | — | Los Angeles-Long Beach-Anaheim, CA | 2914994.474613473 | 368.0 | 0.7098597116468116 |
| LAX-MEL | Los Angeles-Long Beach-Anaheim, CA | — | 2914994.474613473 | 368.0 | 0.7098597116468116 |
| BNE-LAX | — | Los Angeles-Long Beach-Anaheim, CA | 2635424.930187242 | 368.0 | 0.6437704627756154 |
| LAX-BNE | Los Angeles-Long Beach-Anaheim, CA | — | 2635424.930187242 | 368.0 | 0.6437704627756154 |
| ATL-ICN | Atlanta-Sandy Springs-Roswell, GA | — | 2632020.9571328512 | 368.0 | 0.6429671447830597 |


**Suggested CBSA opportunities**

_No data available._


### American Airlines

**Top CBSA corridors**

| Route | Source CBSA Name | Destination CBSA Name | ASM | Total Seats | Performance Score |
| --- | --- | --- | --- | --- | --- |
| IAH-DOH | Houston-Pasadena-The Woodlands, TX | — | 2647862.856012717 | 329.0 | 0.7034176153057455 |
| DOH-IAH | — | Houston-Pasadena-The Woodlands, TX | 2647862.856012717 | 329.0 | 0.7034176153057455 |
| AUH-ORD | — | Chicago-Naperville-Elgin, IL-IN | 2393615.9990285877 | 329.0 | 0.6365668771060771 |
| ORD-AUH | Chicago-Naperville-Elgin, IL-IN | — | 2393615.9990285877 | 329.0 | 0.6365668771060771 |
| ORD-DOH | Chicago-Naperville-Elgin, IL-IN | — | 2346671.4800155116 | 329.0 | 0.6242320599420095 |


**Suggested CBSA opportunities**

_No data available._


## Next Steps

* Run `python3 main.py --airline AIRLINE1 --airline AIRLINE2` for interactive comparisons.
* Seed CBSA data via `python3 main.py --build-cbsa-cache`.
* Use `scripts/showcase.py` for a quick CLI preview.
