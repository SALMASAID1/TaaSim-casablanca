-- TaaSim Cassandra schema (Sprint 1)
--
-- This file is intentionally idempotent:
-- - safe to re-run (uses IF NOT EXISTS)

-- -------------------------------------------------------------------------
-- Keyspace
-- -------------------------------------------------------------------------

CREATE KEYSPACE IF NOT EXISTS taasim
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- -------------------------------------------------------------------------
-- Table: vehicle_positions
-- Query pattern: "list latest vehicle positions in zone X".
--
-- Partition key rationale:
-- - Partitioning by (city, zone_id) groups vehicles by the dashboard/API query key.
-- - Partitioning by taxi_id would make zone queries inefficient (scatter-gather).
--
-- Clustering key rationale:
-- - event_time DESC makes "latest positions" a fast slice read.
-- - taxi_id prevents overwrites when multiple vehicles share the same second-level event_time.
-- - TTL keeps the table small and automatically removes stale positions.
-- -------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS taasim.vehicle_positions (
  city        text,
  zone_id     int,
  event_time  timestamp,
  taxi_id     text,
  lat         double,
  lon         double,
  speed       float,
  status      text,
  PRIMARY KEY ((city, zone_id), event_time, taxi_id)
) WITH CLUSTERING ORDER BY (event_time DESC, taxi_id ASC)
  AND default_time_to_live = 3600;

-- -------------------------------------------------------------------------
-- Table: trips
-- Query pattern: "trip history / monitoring for a city".
--
-- Partition key rationale:
-- - (city, date_bucket) prevents unbounded partition growth.
-- - Without date bucketing, a single city partition would grow forever and hot-spot.
--
-- Clustering key rationale:
-- - created_at DESC supports "most recent trips" queries efficiently.
-- -------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS taasim.trips (
  city                text,
  date_bucket         date,
  created_at          timestamp,
  trip_id             uuid,
  rider_id            text,
  taxi_id             text,
  origin_zone         int,
  dest_zone           int,
  status              text,
  fare                decimal,
  eta_seconds         int,
  matched_within_sla  boolean,      -- Sprint 4: true when eta_seconds ≤ 5
  PRIMARY KEY ((city, date_bucket), created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

-- -------------------------------------------------------------------------
-- Table: kpi_weekly  (Sprint 4, Task 03)
-- Query pattern: "weekly operational KPIs per zone" (Grafana KPI table).
--
-- Partition key rationale:
-- - (city, kpi_name) groups all weekly values for one metric in one partition.
-- - Queries like "all zones for KPI X this week" are a single partition read.
--
-- Clustering key rationale:
-- - week_start DESC + zone_id gives ordered weekly history per zone.
-- -------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS taasim.kpi_weekly (
  city            text,
  kpi_name        text,
  week_start      date,
  zone_id         int,
  zone_name       text,
  metric_value    double,
  detail          text,
  PRIMARY KEY ((city, kpi_name), week_start, zone_id)
) WITH CLUSTERING ORDER BY (week_start DESC, zone_id ASC);

-- -------------------------------------------------------------------------
-- Table: kpi_peak_hours  (Sprint 4, Task 03)
-- Query pattern: "trip count by hour for Grafana bar chart".
-- -------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS taasim.kpi_peak_hours (
  city            text,
  week_start      date,
  zone_id         int,
  hour_of_day     int,
  trip_count      bigint,
  PRIMARY KEY ((city, week_start), hour_of_day, zone_id)
) WITH CLUSTERING ORDER BY (hour_of_day ASC, zone_id ASC);

-- -------------------------------------------------------------------------
-- Table: demand_zones
-- Query pattern: "live supply/demand ratio per zone" (heatmap + KPI panels).
--
-- Partition key rationale:
-- - (city, zone_id) matches the heatmap slice key.
--
-- Clustering key rationale:
-- - window_start DESC makes reading the latest N windows fast.
-- - TTL allows keeping only recent windows (dashboard needs 'now', not all history).
-- -------------------------------------------------------------------------

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
