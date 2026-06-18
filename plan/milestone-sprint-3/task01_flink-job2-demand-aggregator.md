# task01 — Flink Job 2: Demand Aggregator

## Context
Job 2 produces the supply/demand ratio that powers TaaSim's demand heatmap. It ingests two
streams simultaneously — normalised vehicle positions and incoming trip requests — and computes
per-zone ratios every 30 seconds using tumbling windows. The output drives both the Grafana
heatmap in Sprint 3 and the ML forecast overlay added in Sprint 5. Getting the window size and
keying right is critical: a stream not keyed by `zone_id` before windowing will compute global
aggregates, not per-zone.

## Objective
Implement **Flink Job 2 (Demand Aggregator)** in Java under `com.taasim.flink.job2` following the detailed technical specification in [job2-demand-aggregator-spec.md](job2-demand-aggregator-spec.md). It consumes `processed.gps` and `raw.trips`, computes per-zone supply/demand ratio in 30-second tumbling windows, and writes results to Cassandra `demand_zones` and Kafka `processed.demand`.

## Acceptance Criteria
- [ ] Job consumes both `processed.gps` and `raw.trips` with separate Kafka sources and
  named consumer groups (`flink-job2-gps`, `flink-job2-trips`)
- [ ] Both streams use event-time processing with watermarks
- [ ] Stream keyed by `zone_id` before applying tumbling window
- [ ] Tumbling window size = 30 seconds
- [ ] Per window: `active_vehicles` = distinct `taxi_id` count, `pending_requests` = trip event count
- [ ] `ratio = pending_requests / max(active_vehicles, 1)` computed and included in output
- [ ] Results upserted to Cassandra `taasim.demand_zones` using `(zone_id, window_start)` as
  composite key (idempotent write — no duplicates on replay)
- [ ] Results also published to Kafka topic `processed.demand`
- [ ] Grafana demand heatmap (task03) updates with fresh data at least every 35 seconds
- [ ] Job runs stably alongside Job 1 for 10 minutes without restarts

## Technical Hints
- Connect two Kafka sources and union or join them into a common stream before keying.
  For this use case, a simple union on a normalised event type (tagging each event as `GPS` or
  `TRIP`) followed by `keyBy(zone_id)` is cleanest.
- Tumbling window:
  ```python
  stream.key_by(lambda e: e['zone_id']) \
        .window(TumblingEventTimeWindows.of(Time.seconds(30))) \
        .process(DemandAggregateFunction())
  ```
- Inside `DemandAggregateFunction`, maintain two accumulators: a `set` for vehicle IDs and a
  counter for trip requests.
- Cassandra upsert: use `INSERT INTO ... IF NOT EXISTS` or rely on Cassandra's last-write-wins
  for the same primary key — both achieve idempotency here.
- Reference: project brief §3.3 Flink Processing Jobs (Job 2), §9.3 Flink Jobs.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
