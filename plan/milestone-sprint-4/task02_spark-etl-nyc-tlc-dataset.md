# task02 — Spark ETL: NYC TLC Dataset

## Context
The NYC TLC dataset (3 months, ~30 M rows) is TaaSim's large-scale Spark exercise. It is not
used for streaming — its sole purpose is to force real Spark optimisation work: partitioning
strategies, broadcast joins, and columnar Parquet reads. The per-zone-per-hour demand aggregates
it produces feed into the Grafana KPI analytics panel and complement the Porto ML features with
a richer demand signal. Processing 10 M rows per month cleanly within time and memory budget
is the primary deliverable.

## Objective
Implement `spark_jobs/etl_nyc_tlc.py` that reads 3 months of NYC TLC Parquet from MinIO,
computes per-zone-per-hour demand aggregates, and writes the results to
`s3a://taasim/curated/demand-by-zone/`.

## Acceptance Criteria
- [ ] Spark job reads from `s3a://taasim/raw/nyc-tlc/` (3 months of Yellow Taxi Parquet files)
- [ ] Per-zone-per-hour demand aggregates computed:
  `(pickup_location_id, hour_of_day, day_of_week) → trip_count, avg_trip_distance, avg_fare`
- [ ] Results written as Parquet to `s3a://taasim/curated/demand-by-zone/`
- [ ] Job processes ≥ 10 M rows/month without OOM errors on a single Spark worker (4 GB RAM)
- [ ] Spark UI shows no single stage taking more than 3 minutes (partitioning tuned)
- [ ] At least one broadcast join used (for the zone reference table) — documented in code comments
- [ ] `spark.read.parquet("s3a://taasim/curated/demand-by-zone/")` readable and row count logged
- [ ] Spark UI screenshots for the NYC job captured and committed to `docs/spark-nyc-ui.png`

## Technical Hints
- NYC TLC Parquet fields used: `tpep_pickup_datetime`, `PULocationID`, `trip_distance`,
  `fare_amount`, `passenger_count`.
- Extract hour and day from pickup datetime:
  ```python
  from pyspark.sql.functions import hour, dayofweek, col
  df = df.withColumn("hour_of_day", hour("tpep_pickup_datetime")) \
         .withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
  ```
- Aggregation:
  ```python
  agg_df = df.groupBy("PULocationID", "hour_of_day", "day_of_week") \
             .agg(count("*").alias("trip_count"),
                  avg("trip_distance").alias("avg_distance"),
                  avg("fare_amount").alias("avg_fare"))
  ```
- To avoid 200-partition shuffle overhead on a local machine:
  `spark.conf.set("spark.sql.shuffle.partitions", "16")`
- Read all 3 months in one call using a wildcard path:
  `spark.read.parquet("s3a://taasim/raw/nyc-tlc/*.parquet")`
- Reference: project brief §2.2 NYC TLC Trip Records, §9.4 Spark Jobs.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
Implemented in `spark_jobs/etl_nyc_tlc.py`. Reads 3 months of Yellow Taxi Parquet (~30M rows), computes per-zone-per-hour demand aggregates using broadcast join with zone_mapping.csv. Uses PULocationID % 16 + 1 for zone mapping. Optimised with spark.sql.shuffle.partitions=16.
