#!/usr/bin/env bash
set -euo pipefail
OUTDIR="data/transtats_downloads"
mkdir -p "$OUTDIR"

# Adjust ranges as needed
START_YEAR=2023
END_YEAR=2024

# Download DB1B Market (quarterly)
echo "Downloading DB1B Market..."
for Y in $(seq $START_YEAR $END_YEAR); do
  for Q in 1 2 3 4; do
    URL="https://transtats.bts.gov/PREZIP/Origin_and_Destination_Survey_DB1BMarket_${Y}_${Q}.zip"
    FILE="$OUTDIR/DB1BMarket_${Y}_Q${Q}.zip"
    echo "  $URL -> $FILE"
    curl -fL "$URL" -o "$FILE" || echo "    Failed: $URL"
  done
done

# Download T-100 Segment and Market (monthly, all carriers)
echo "Downloading T-100 Segment/Market..."
for Y in $(seq $START_YEAR $END_YEAR); do
  for M in $(seq 1 12); do
    URL_SEG="https://transtats.bts.gov/PREZIP/T_T100D_SEGMENT_ALL_CARRIER_${Y}_${M}.zip"
    FILE_SEG="$OUTDIR/T100_SEG_${Y}_${M}.zip"
    echo "  $URL_SEG -> $FILE_SEG"
    curl -fL "$URL_SEG" -o "$FILE_SEG" || echo "    Failed: $URL_SEG"

    URL_MKT="https://transtats.bts.gov/PREZIP/T_T100D_MARKET_ALL_CARRIER_${Y}_${M}.zip"
    FILE_MKT="$OUTDIR/T100_MKT_${Y}_${M}.zip"
    echo "  $URL_MKT -> $FILE_MKT"
    curl -fL "$URL_MKT" -o "$FILE_MKT" || echo "    Failed: $URL_MKT"
  done
done

