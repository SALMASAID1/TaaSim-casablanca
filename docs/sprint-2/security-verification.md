# Security Verification — GPS Anonymization (Sprint 2)

## Goal
Verify that **raw GPS lat/lon** published to `raw.gps` are **never persisted** in Cassandra.

## Method
- Publish a small batch of GPS events with known raw coordinates
- Wait for Flink Job 1 to process them
- Query Cassandra `taasim.vehicle_positions`

## Assertions
- [ ] No stored row matches any raw input coordinate (tolerance ≤ 1m)
- [ ] All stored coordinates match one of the 16 zone centroid coordinates (tolerance ≤ 1m)

## Evidence
- [ ] Test output (pytest) attached/logged
- [ ] Cassandra query sample results
