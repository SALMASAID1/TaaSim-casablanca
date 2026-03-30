# task02 — Flink Job 3: Trip Matcher

## Context
Job 3 is TaaSim's core product logic: matching a rider's reservation to the nearest available
vehicle within 5 seconds. It is the most complex Flink job in the platform — it requires stateful
processing (tracking available vehicles per zone), a SLA timer (emit `unmatched` if no vehicle
found in 5 s), and an adjacent-zone fallback (expanded in task05). This job's output is what
investors see when a trip reservation triggers a match event on the demo dashboard.

## Objective
Implement `flink_jobs/job3_trip_matcher.py` that matches incoming trip requests from `raw.trips`
to available vehicles tracked via Flink keyed state, computes ETA, writes match records to
Cassandra `trips`, and emits an `unmatched` event if no vehicle is found within 5 seconds.

## Acceptance Criteria
- [ ] Job consumes `raw.trips` with consumer group `flink-job3-trips`
- [ ] Vehicle availability tracked in Flink **keyed state** (keyed by `zone_id`), updated by
  consuming `processed.gps` as a second source
- [ ] **RocksDB** state backend configured (required for production-grade state size)
- [ ] Matching logic: select vehicle in same zone with `status=available` and oldest `last_seen`
  timestamp
- [ ] On match: vehicle status updated to `assigned` in state; match event emitted with
  `trip_id`, `taxi_id`, `eta_seconds = distance_km / avg_speed_kmh * 3600`
- [ ] Trip record written to Cassandra `taasim.trips` with `IF NOT EXISTS` (idempotent)
- [ ] **SLA enforcement**: if no match found within 5 seconds of event time, emit `unmatched`
  event to Kafka topic `raw.unmatched` for monitoring
- [ ] End-to-end latency measured: `POST /api/v1/trips` → match record visible in Cassandra
  must be < 5 seconds P95 (measured and documented)
- [ ] Job runs stably for 10 minutes alongside Jobs 1 and 2

## Technical Hints
- Use a `KeyedProcessFunction` keyed by `zone_id` for the matching logic. Register a
  processing-time timer for the 5-second SLA:
  ```python
  ctx.timer_service().register_processing_time_timer(
      ctx.timer_service().current_processing_time() + 5000
  )
  ```
  In `on_timer()`, if the trip is still unmatched, emit an `unmatched` event.
- Vehicle state: use `MapState[taxi_id, VehicleInfo]` within the `KeyedProcessFunction`.
  Update vehicle state by processing `processed.gps` events in the same function (broadcast or
  co-process pattern).
- ETA calculation: simple Euclidean distance on zone centroid coordinates converted to km, divided
  by average speed (use 25 km/h as default for Casablanca traffic).
- RocksDB backend:
  ```python
  env.set_state_backend(EmbeddedRocksDBStateBackend())
  ```
- Reference: project brief §3.3 Flink Processing Jobs (Job 3), §6.1 Performance & Latency,
  §9.3 Flink Jobs.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
