# task02 — SLA Measurement Report

## Context
The project brief states that "it feels fast" is not an acceptable measurement. Every performance
requirement in §6.1 has a concrete measurement method and a numeric target. The SLA
measurement table must appear in the technical report — teams that skip it lose marks in both
the Engineering Quality and Technical Report pillars. This task runs the full platform under load
and captures each metric against its target.

## Objective
Measure all five SLA targets from §6.1 of the project brief under simultaneous load, record the
results in a structured table, and identify any targets that were not met with an honest
explanation.

## Acceptance Criteria
- [ ] **Integration test run**: all 3 Flink jobs + Spark ETL + FastAPI running simultaneously for
  30 minutes continuously without crashes
- [ ] **SLA 1 — Trip match latency P95**: measured by comparing Kafka `raw.trips` message
  timestamp to Cassandra `trips.created_at` timestamp; target < 5 seconds; ≥ 100 samples
- [ ] **SLA 2 — Vehicle position freshness**: measured by comparing Kafka `raw.gps` producer
  timestamp to Cassandra `vehicle_positions.event_time`; target < 15 seconds; ≥ 100 samples
- [ ] **SLA 3 — Demand zone update frequency**: verified using Cassandra `WRITETIME()` on
  `demand_zones`; target: new row every 30 seconds; ≥ 10 samples per zone
- [ ] **SLA 4 — ML forecast API response time**: measured with Locust at 20 req/s for 60 seconds;
  target P95 < 500 ms; Locust HTML report saved
- [ ] **SLA 5 — Spark ETL duration**: Porto 1.7 M rows processed in < 5 minutes; Spark UI
  job duration screenshot saved
- [ ] Results table committed to `docs/sla-measurement-table.md` in this exact format:

  | SLA | Target | Measured (P95) | Pass/Fail | Notes |
  |-----|--------|----------------|-----------|-------|
  | Trip match latency | < 5 s | ? | ? | |
  | Vehicle position freshness | < 15 s | ? | ? | |
  | Demand zone update freq | 30 s | ? | ? | |
  | ML forecast latency | < 500 ms | ? | ? | |
  | Spark ETL (Porto) | < 5 min | ? | ? | |

- [ ] Any failed SLA documented with root cause and proposed fix

## Technical Hints
- Latency measurement script `tests/measure_latency.py` (write it):
  - Produce 100 trip request events with known `requested_at` timestamps to `raw.trips`
  - Poll Cassandra `trips` table every 500 ms for up to 30 s per event
  - Record `created_at - requested_at` in milliseconds for each event
  - Compute P50, P95, P99 and print
- WRITETIME() query in cqlsh:
  ```cql
  SELECT zone_id, window_start, WRITETIME(active_vehicles) AS write_ts
  FROM taasim.demand_zones
  WHERE city='casablanca' AND zone_id=1
  LIMIT 20;
  ```
  Diff consecutive `write_ts` values to verify 30-second cadence.
- Locust run:
  ```bash
  locust -f tests/locustfile.py --headless -u 20 -r 2 -t 60s \
    --host http://localhost:8000 --html docs/locust-report.html
  ```
- Reference: project brief §6.1 Performance & Latency (full table), §8 Evaluation Rubric
  (Distinction Level — "All SLA targets met and measured").

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
