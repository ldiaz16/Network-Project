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

## Competing Route Overlap


| Source | Dest | deltalines | american |
| --- | --- | --- | --- |
| ATL | CLT | 0.0 | 0.0 |
| ATL | DFW | 0.0 | 0.0 |
| ATL | LHR | 0.0 | 0.0 |
| ATL | MIA | 0.0 | 0.0 |
| ATL | ORD | 0.0 | 0.0 |


## CBSA Opportunity Highlights

### Delta Air Lines

**Top CBSA corridors**

| Route | Source CBSA Name | Destination CBSA Name | ASM | Total Seats | Performance Score |
| --- | --- | --- | --- | --- | --- |
| ATL-FCO | Atlanta-Sandy Springs-Roswell, GA | non-US | 1414966.4958461216 | 281.0 | 0.7195949521108074 |
| FCO-ATL | non-US | Atlanta-Sandy Springs-Roswell, GA | 1414966.4958461216 | 281.0 | 0.7195949521108074 |
| AMS-PDX | non-US | Portland-Vancouver-Hillsboro, OR-WA | 1406000.9782794781 | 281.0 | 0.7152845580543193 |
| PDX-AMS | Portland-Vancouver-Hillsboro, OR-WA | non-US | 1406000.9782794781 | 281.0 | 0.7152845580543193 |
| AMS-SEA | non-US | Seattle-Tacoma-Bellevue, WA | 1373035.2575323912 | 281.0 | 0.6994495040415195 |


**Suggested CBSA opportunities**

| Proposed Source | Proposed Destination | Source CBSA | Destination CBSA | Reference Route | Opportunity Score |
| --- | --- | --- | --- | --- | --- |
| YEG | PWM | non-US | Portland-Vancouver-Hillsboro, OR-WA | AMS-PDX | 0.429 |
| ATL | YEG | Atlanta-Sandy Springs-Roswell, GA | non-US | ATL-FCO | 0.426 |
| YEG | ATL | non-US | Atlanta-Sandy Springs-Roswell, GA | FCO-ATL | 0.426 |
| \\N | YHZ | Portland-Vancouver-Hillsboro, OR-WA | non-US | PDX-AMS | 0.426 |
| YEG | FTY | non-US | Atlanta-Sandy Springs-Roswell, GA | FCO-ATL | 0.425 |


### American Airlines

**Top CBSA corridors**

| Route | Source CBSA Name | Destination CBSA Name | ASM | Total Seats | Performance Score |
| --- | --- | --- | --- | --- | --- |
| ABE-CLT | Allentown-Bethlehem-Easton, PA-NJ | Charlotte-Concord-Gastonia, NC-SC | 0.0 | 0.0 | 0.0 |
| ORD-BNA | Chicago-Naperville-Elgin, IL-IN | Nashville-Davidson--Murfreesboro--Franklin, TN | 0.0 | 0.0 | 0.0 |
| OMA-ORD | Omaha, NE-IA | Chicago-Naperville-Elgin, IL-IN | 0.0 | 0.0 | 0.0 |
| OMA-PHX | Omaha, NE-IA | Phoenix-Mesa-Chandler, AZ | 0.0 | 0.0 | 0.0 |
| ONT-DFW | Riverside-San Bernardino-Ontario, CA | Dallas-Fort Worth-Arlington, TX | 0.0 | 0.0 | 0.0 |


**Suggested CBSA opportunities**

| Proposed Source | Proposed Destination | Source CBSA | Destination CBSA | Reference Route | Opportunity Score |
| --- | --- | --- | --- | --- | --- |
| ABE | USA | Allentown-Bethlehem-Easton, PA-NJ | Charlotte-Concord-Gastonia, NC-SC | ABE-CLT | 0.0 |
| ABE | CCR | Allentown-Bethlehem-Easton, PA-NJ | Charlotte-Concord-Gastonia, NC-SC | ABE-CLT | 0.0 |
| ESN | CLT | Allentown-Bethlehem-Easton, PA-NJ | Charlotte-Concord-Gastonia, NC-SC | ABE-CLT | 0.0 |
| MDW | BNA | Chicago-Naperville-Elgin, IL-IN | Nashville-Davidson--Murfreesboro--Franklin, TN | ORD-BNA | 0.0 |
| MDW | FKL | Chicago-Naperville-Elgin, IL-IN | Nashville-Davidson--Murfreesboro--Franklin, TN | ORD-BNA | 0.0 |


## Next Steps

* Run `python3 main.py --airline AIRLINE1 --airline AIRLINE2` for interactive comparisons.
* Seed CBSA data via `python3 main.py --build-cbsa-cache`.
* Use `scripts/showcase.py` for a quick CLI preview.
