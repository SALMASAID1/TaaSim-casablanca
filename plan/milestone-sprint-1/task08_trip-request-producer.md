# task08 — Trip Request Producer

## Context
The official course brief requires a realistic stream of citizen trip reservation events on Kafka
(topic `raw.trips`). This stream is an input for **Flink Job 2 (Demand Aggregator)** and
**Flink Job 3 (Trip Matcher)**. Building it early also enables Week 1–2 verification:
Kafka topics, JSON payload shape, and Kafka → MinIO archival under `raw/kafka-archive/`.

## Objective
Build `src/producers/trip_request_producer.py` that emits trip reservation events to Kafka topic
`raw.trips` following Porto's hourly demand curve, with correct peak/off-peak ratios and
Friday/Sunday pattern adjustments.

## Acceptance Criteria
- [x] Script emits events to Kafka topic `raw.trips` continuously
- [x] Each event contains: `trip_id` (UUID), `rider_id`, `origin_zone` (1–16), `destination_zone`
  (1–16), `requested_at` (event-time ISO-8601), `call_type` (A/B/C — sampled from Porto
  distribution)
- [x] Emission rate follows Porto demand curve: peak hours (7–9h, 17–19h) produce ≥ 3× the
  off-peak rate
- [x] Friday 12–14h rate is reduced to ≈ 0.7× normal (jumu'ah pattern)
- [x] Sunday rate is reduced to ≈ 0.6× normal (weekly low)
- [x] Demand multiplier per hour pre-computed from Porto dataset and stored as a list in the script
- [x] Script is runnable with `python -m src.producers.trip_request_producer --speed 10`
- [x] `kafka-console-consumer --topic raw.trips` shows valid JSON messages at varying rates

## Technical Hints
- Compute the hourly multiplier from the Porto `train.csv` file in a separate notebook step:
  ```python
  hourly_counts = df.groupBy(hour('TIMESTAMP')).count()
  max_count = hourly_counts.agg(max('count')).collect()[0][0]
  multipliers = {row['hour(TIMESTAMP)']: row['count']/max_count for row in hourly_counts.collect()}
  ```
  Hard-code the resulting dict/list in the producer script (24 values).
- Base emission rate at off-peak: 1 event per second. Scale by the multiplier for each simulated
  hour of day.
- `call_type` distribution from Porto: approximately A=35%, B=40%, C=25%.
- Zone pair `(origin_zone, destination_zone)` can be sampled from the Porto trip OD matrix
  (aggregate `origin_zone`/`dest_zone` counts from the coordinate-transformed dataset) or sampled
  uniformly for simplicity.
- Reference: course brief §2.3 (Simulation Layer) and §9.2 (Data Producers).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
