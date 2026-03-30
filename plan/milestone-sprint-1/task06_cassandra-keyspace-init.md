# task06 — Cassandra Keyspace Init

## Context
Cassandra is TaaSim's low-latency serving layer. Every query pattern — nearest vehicles in a
zone, trip history by day, live demand heatmap — is baked into the table schema through partition
key and clustering key choices. Designing the schema in Sprint 1 means Flink jobs, the FastAPI
service, and Grafana panels all have stable targets to write to and query from. Getting partition
keys wrong causes hotspots and slow queries that will fail SLA checks in Sprint 6.

## Objective
Create the Cassandra keyspace `taasim` and deploy the three required tables (`vehicle_positions`,
`trips`, `demand_zones`) with documented partition key rationale.

## Acceptance Criteria
- [ ] Keyspace `taasim` created with `replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`
- [ ] Table `vehicle_positions` created with partition key `(city, zone_id)`, clustering key
  `event_time DESC`
- [ ] Table `trips` created with partition key `(city, date_bucket)`, clustering key `created_at DESC`
- [ ] Table `demand_zones` created with partition key `(city, zone_id)`, clustering key
  `window_start DESC`
- [ ] Each table's partition key choice justified in a written comment block within the CQL init file
- [ ] Init script `db/cassandra_init.cql` committed to the repository
- [ ] `cqlsh -f db/cassandra_init.cql` runs idempotently (use `IF NOT EXISTS` on all DDL)
- [ ] A brief Architecture Decision Record (ADR) added to `docs/adr/adr-001-cassandra-schema.md`
  covering: why `(city, zone_id)` and not `taxi_id`, and why `date_bucket` prevents unbounded
  partition growth

## Technical Hints
- Minimum CQL schema:
  ```cql
  CREATE TABLE IF NOT EXISTS taasim.vehicle_positions (
    city        text,
    zone_id     int,
    event_time  timestamp,
    taxi_id     text,
    lat         double,
    lon         double,
    speed       float,
    status      text,
    PRIMARY KEY ((city, zone_id), event_time)
  ) WITH CLUSTERING ORDER BY (event_time DESC)
    AND default_time_to_live = 3600;

  CREATE TABLE IF NOT EXISTS taasim.trips (
    city         text,
    date_bucket  date,
    created_at   timestamp,
    trip_id      uuid,
    rider_id     text,
    taxi_id      text,
    origin_zone  int,
    dest_zone    int,
    status       text,
    fare         decimal,
    eta_seconds  int,
    PRIMARY KEY ((city, date_bucket), created_at)
  ) WITH CLUSTERING ORDER BY (created_at DESC);

  CREATE TABLE IF NOT EXISTS taasim.demand_zones (
    city               text,
    zone_id            int,
    window_start       timestamp,
    active_vehicles    int,
    pending_requests   int,
    ratio              float,
    forecast_demand    float,
    PRIMARY KEY ((city, zone_id), window_start)
  ) WITH CLUSTERING ORDER BY (window_start DESC)
    AND default_time_to_live = 86400;
  ```
- `date_bucket` for `trips` prevents partition explosion: one partition per (city, day) →
  bounded growth, efficient range scans.
- `default_time_to_live = 3600` on `vehicle_positions` auto-expires stale rows after 1 hour.
- Reference: project brief §4.1 Required Tables, §4 Data Model — Cassandra Schema.

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
