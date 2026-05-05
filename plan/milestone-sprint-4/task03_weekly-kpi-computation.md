# task03 — Weekly KPI Computation (Spark SQL Analytics)

## Context
TaaSim's value to city planners is not just real-time matching — it is the analytical layer that
reveals structural patterns: which zones are chronically underserved, what peak hours look like
across arrondissements, and where supply-demand gaps are widest. These KPIs are computed
weekly from the curated Porto dataset using Spark SQL and loaded into Cassandra to power the
Grafana KPI table panel. They also appear in the technical report as evidence of the platform's
business intelligence capabilities.

## Objective
Implement `spark_jobs/kpi_weekly.py` that computes four weekly KPIs from `curated/porto-trips/`
using Spark SQL and writes the results to Cassandra `demand_zones` for Grafana display.

## Acceptance Criteria
- [ ] KPI 1 — **Trips per zone**: total trip count per `arrondissement_id` per week
- [ ] KPI 2 — **Average trip duration**: mean trip duration in minutes per zone per week
  (computed from POLYLINE point count × 15 seconds per GPS interval)
- [ ] KPI 3 — **Peak demand hours**: top 3 hours by trip count per zone, per week
- [ ] KPI 4 — **Coverage gap**: zones where weekly demand > threshold AND average active
  vehicles < 2 (under-supplied zones) — list with zone name and gap score
- [ ] All four KPIs computed and printed to Spark console without errors
- [ ] KPI aggregates written to Cassandra `taasim.demand_zones` (use `forecast_demand` field or
  add a separate KPI table if schema change is justified in ADR)
- [ ] Spark SQL queries used (not just DataFrame API) — at least 2 KPIs use `spark.sql()`
- [ ] Notebook `notebooks/04_kpi_analysis.ipynb` with visualisations of all 4 KPIs committed

## Technical Hints
- Trip duration from POLYLINE: each GPS ping is separated by 15 seconds in Porto data.
  ```python
  # After explode, count points per trip, multiply by 15s
  duration_df = df.groupBy("TRIP_ID").agg(count("coord").alias("gps_count"))
  duration_df = duration_df.withColumn("duration_min", col("gps_count") * 15 / 60)
  ```
- Coverage gap query in Spark SQL:
  ```sql
  SELECT zone_id, zone_name, weekly_trips, avg_active_vehicles,
         (weekly_trips - avg_active_vehicles * 100) AS gap_score
  FROM zone_kpi_weekly
  WHERE avg_active_vehicles < 2 AND weekly_trips > 50
  ORDER BY gap_score DESC
  ```
- For writing KPIs to Cassandra, use the `spark-cassandra-connector`:
  ```python
  kpi_df.write \
      .format("org.apache.spark.sql.cassandra") \
      .options(table="demand_zones", keyspace="taasim") \
      .mode("append").save()
  ```
- Reference: project brief §7 Weekly Lab Plan (W5 tasks), §9.4 Spark Jobs.

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
Implemented in `spark_jobs/kpi_weekly.py`. All 4 KPIs computed: trips/zone (Spark SQL), avg duration, peak hours (Spark SQL + ROW_NUMBER window), coverage gap. Added kpi_weekly and kpi_peak_hours tables to cassandra_init.cql. Cassandra write with Parquet fallback.
