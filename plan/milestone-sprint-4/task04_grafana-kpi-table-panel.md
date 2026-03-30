# task04 — Grafana KPI Table Panel

## Context
Beyond the real-time heatmap, TaaSim's dashboard needs a summary KPI table showing the
operational health of the platform over the last 24 hours. This panel is what a fleet manager
reads first thing in the morning. It shows total trips completed, average ETA, percentage of trips
matched within the 5-second SLA, and the top three demand zones. The figures must update
whenever Cassandra is written to — they should never be static snapshots.

## Objective
Add a KPI Table panel and a Peak Hours bar chart to the Grafana `taasim-live` dashboard,
sourcing data from the Cassandra `demand_zones` table populated by the Spark KPI job.

## Acceptance Criteria
- [ ] **Panel 3 — KPI Table**: Grafana Table panel showing:
  - Total trips in last 24 hours
  - Average ETA (seconds) in last 24 hours
  - % of trips matched within 5 seconds (from `trips` table)
  - Top 3 demand zones by `pending_requests` (from `demand_zones`)
- [ ] **Panel 4 — Peak Hours Bar Chart**: bar chart showing trip count by hour of day
  (0–23) for the current week, sourced from Spark KPI output in Cassandra
- [ ] KPI Table panel refreshes when `demand_zones` receives a new Spark write (auto-refresh
  every 60 seconds is acceptable for batch-computed KPIs)
- [ ] All figures are correct (manually verified against `cqlsh` queries)
- [ ] Dashboard JSON re-exported to `grafana/dashboards/taasim-live.json`
- [ ] Screenshot of completed 4-panel dashboard committed to `docs/grafana-dashboard.png`

## Technical Hints
- Total trips query (Cassandra):
  ```
  SELECT COUNT(*) FROM taasim.trips
  WHERE city='casablanca' AND date_bucket=toDate(now())
  ```
- % matched within 5s: add a `matched_within_sla` boolean column to the `trips` table
  (set to `true` in Flink Job 3 when `eta_seconds ≤ 5`), then:
  ```
  SELECT COUNT(*) FILTER (WHERE matched_within_sla=true) / COUNT(*) FROM taasim.trips ...
  ```
  Note: Cassandra does not support `FILTER` — compute this ratio in a Grafana transformation
  using two separate queries (count matched vs total) and a `Math` transform.
- For the bar chart panel, use Grafana's **Bar chart** panel type with `hour_of_day` on X-axis
  and `trip_count` on Y-axis. The Cassandra query returns 24 rows, one per hour.
- Reference: project brief §9.6 Grafana Dashboard (Panel 3), §7 Weekly Lab Plan (W5).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
