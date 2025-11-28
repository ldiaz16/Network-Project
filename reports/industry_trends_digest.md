# Industry trends digest (from resources/)

This note summarizes every resource file in `resources/` to ground the app’s metrics and UI copy in current airline trends.

## Fleet utilization — Q2 2024 (28Fleet Utilization Q2 2024 - Website)
- Focus: narrowbody fleets; cycles per aircraft over the last 12 months (ch-aviation + Milanamos).
- Higher average cycles indicate harder-working fleets; pair with average stage length to detect over/under-use.
- Lessors’ portfolios show wide dispersion by type; use as a benchmark for utilization scoring.

## Network aggressiveness — Nov 2024 (29Network Aggressiveness - Website)
- Measures share of capacity on monopoly/duopoly/multi-competitor routes by fleet type.
- Examples: Lufthansa A321neo ~37% monopoly vs. Vistara A321neo ~95% competitive; Ethiopian 737-8 mostly monopoly vs. Akasa fully competitive.
- Score centered at ~5; higher = more favorable (more monopoly exposure).

## Network maturity — Q2 2024 (30Network Maturity Q2 2024 - Website)
- Stable networks keep most routes >3 years; fluid networks have large portions <12 months.
- Examples: Turkish A321neo network very stable; Wizz A321neo highly fluid with many <12-month routes.
- Score 0–10 benchmarked on 1,500 airline/aircraft combos.

## Airline network strategy — Q2 2023–Q2 2024
- Compares capacity growth vs. % seats on new routes across regions (US, Canada, Europe, Middle East, South America, China, SE Asia).
- Used to classify carriers into Network Freeze, Shift, Entrenchment, Expansion bands.

## Airline network strategy since COVID — Oct 2023
- Network Strategy Matrix definitions:
  - Freeze: low route renewal (<10%), capacity flat.
  - Shift: capacity flat but routes rotate.
  - Entrenchment: more seats on existing network to defend share.
  - Expansion: more seats on new routes; playing offense.
- Regional takeaways: Ryanair still disruptor; Iberia strong margins/OTP; Gulf LCCs drift toward Freeze as they age; SE Asia recovery uneven (Thai/Garuda lag, AirAsia behind pre-COVID network).

## Market share evolution — Jan 2024
- Combines capacity and pricing moves to see passenger/revenue share changes (top 20 revenue routes as proxy).
- Europe: Ryanair/Wizz/Transavia France/Volotea gain share; easyJet/Vueling lose passenger share but defend revenue via higher fares; Iberia gains with lower fares than peers despite hikes; Lufthansa trades some revenue for passengers; Air France holds revenue, loses passengers as some move to Transavia.
- Middle East: Emirates sets pace; Qatar fastest capacity ramp (share gains, potential overcapacity); Etihad smaller after restructuring; Turkish flexible mix; Saudia balancing flyadeal vs. flynas.
- North America: United ramped capacity fast and held share; American raised fares more and lost share; Southwest gained passengers with lower fares; Frontier vs. Spirit shows fare-hike penalty for Spirit.
- LCC pricing: Frontier moderate hikes > better share; Spirit big hikes > share loss; ULCC fare strategy matters.

## Ryanair vs. easyJet — European domestic market
- Ryanair remains the growth aggressor; easyJet sits closer to Freeze/Shift with slower new-route velocity; informs expectations for market-share and maturity dynamics.

## TOP 30 O&Ds by ASK (2023)
- Heaviest corridors remain London–NYC, LAX–NYC, Dubai–London, London–Singapore, LAX–Tokyo, etc.
- Largest operator seat shares range ~20–80%; useful for validating market-share outputs and benchmarking ASM-heavy routes.

## Ticket Revenue Increase — post-COVID
- Global fares up; LCCs captured largest uplift. Exceptions: flyadeal and flynas lowered fares.
- Regional notes: Air Serbia highest increase in Europe; Lufthansa/Swiss/KLM among highest fares when normalized for sector length; Sunwing only NA carrier with lower revenue per pax pre-acquisition; Frontier remains lowest fares in US; Volaris lowest in Mexico; West Air/Lucky Air large hikes in China; India LCCs limited fare growth.

## PDFs in resources/
- PDF counterparts mirror the text files above (network strategy, aggressiveness, maturity, revenue, market share, airline earnings releases). Key quantitative narratives are reflected in the bullets above.

## How this informs the app
- Competition/maturity labels now match percentile-based scoring and can be interpreted against these industry bands.
- Fleet assignment and “optimal aircraft” scoring can reference equipment-level aggressiveness/utilization benchmarks.
- Route/CBSA opportunities can be weighted with awareness of current fare/market-share trends (e.g., aggressive LCC corridors vs. contracting markets).
