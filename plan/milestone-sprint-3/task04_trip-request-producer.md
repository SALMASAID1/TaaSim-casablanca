# task04 — Trip Request Producer

## Context
Job 3 and the demand heatmap both require a realistic stream of citizen trip reservation events.
The `trip_request_producer.py` simulates this by following Porto's historical demand curve,
ensuring the platform experiences realistic peak-hour load (7–9 am, 5–7 pm) and quieter
off-peak periods. Without this producer, Jobs 2 and 3 have zero input on the `raw.trips` topic
and the dashboard shows empty heatmaps.

## Objective
Build `producers/trip_request_producer.py` that emits trip reservation events to Kafka topic
`raw.trips` following Porto's hourly demand curve, with correct peak/off-peak ratios and
Friday/Sunday pattern adjustments.

## Acceptance Criteria
- [ ] Script emits events to Kafka topic `raw.trips` continuously
- [ ] Each event contains: `trip_id` (UUID), `rider_id`, `origin_zone` (1–16), `destination_zone`
  (1–16), `requested_at` (event-time ISO-8601), `call_type` (A/B/C — sampled from Porto
  distribution)
- [ ] Emission rate follows Porto demand curve: peak hours (7–9h, 17–19h) produce ≥ 3× the
  off-peak rate
- [ ] Friday 12–14h rate is reduced to ≈ 0.7× normal (jumu'ah pattern)
- [ ] Sunday rate is reduced to ≈ 0.6× normal (weekly low)
- [ ] Demand multiplier per hour pre-computed from Porto dataset and stored as a list in the script
- [ ] Script is runnable with `python producers/trip_request_producer.py --speed 10`
- [ ] `kafka-console-consumer --topic raw.trips` shows valid JSON messages at varying rates

## Technical Hints
- Compute the hourly multiplier from the Porto `train.csv` file in a separate notebook step:
  ```python
  hourly_counts = df.groupBy(hour('TIMESTAMP')).count()
  max_count = hourly_counts.agg(max('count')).collect()[0][0]
  multipliers = {row['hour(TIMESTAMP)']: row['count']/max_count for row in hourly_counts.collect()}
  ```
  Hard-code the resulting dict in the producer script (24 values).
- Base emission rate at off-peak: 1 event per second. Scale by the multiplier for each simulated
  hour of day.
- `call_type` distribution from Porto: approximately A=35%, B=40%, C=25%.
- Zone pair `(origin_zone, destination_zone)` can be sampled from the Porto trip OD matrix
  (aggregate `origin_zone`/`dest_zone` counts from the coordinate-transformed dataset in
  Sprint 1) or sampled uniformly for simplicity.
- Reference: project brief §9.2 Data Producers (trip_request_producer.py row).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
