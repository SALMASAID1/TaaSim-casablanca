# ADR-001 — Cassandra schema & partition keys

## Status
Accepted — 2026-04-19

## Context
TaaSim needs a low-latency serving store for:

- Live vehicle positions (for a map/dashboard)
- Trip lifecycle / trip history
- Live demand aggregates per zone (heatmap)

In Cassandra, the **primary key is the query plan**: partition keys must match the main access patterns.

## Decision
We create keyspace `taasim` and three tables:

1) `vehicle_positions` with primary key `((city, zone_id), event_time)` and clustering order `event_time DESC`.

- Why `(city, zone_id)` and not `taxi_id`?
  - The expected query is “show me vehicles in zone X now” (dashboard + API).
  - Partitioning by `taxi_id` would make this query a scatter-gather across many partitions.
  - Clustering by `event_time DESC` makes “latest positions” a fast slice read.
  - TTL (1 hour) prevents the table from accumulating stale positions.

2) `trips` with primary key `((city, date_bucket), created_at)` and clustering order `created_at DESC`.

- Why `date_bucket`?
  - Trip history is naturally queried by time ranges (e.g., “today”, “last 24h”, “this week”).
  - A single partition per city would grow without bound and eventually hot-spot.
  - Bucketing by day keeps partitions bounded and predictable.

3) `demand_zones` with primary key `((city, zone_id), window_start)` and clustering order `window_start DESC`.

- Why `(city, zone_id)`?
  - The heatmap/KPI pattern reads “latest windows for zone X” (or scans recent zones).
  - TTL (24 hours) keeps only the recent windows used by the dashboard.

The idempotent DDL is kept in `db/cassandra_init.cql` and applied by the Compose init job.

## Consequences
- Fast reads for the dashboard/API patterns (zone-centric reads).
- Writers (Flink/FastAPI) must always provide `city` and the appropriate bucketing fields.
- Historical retention is controlled via TTLs; long-term analytics belongs in MinIO (raw/curated zones).
