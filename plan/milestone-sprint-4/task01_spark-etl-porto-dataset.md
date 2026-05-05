# task01 — Spark ETL: Porto Dataset

## Context
The ML pipeline in Sprint 5 trains on cleaned, geo-enriched historical Porto trip data. If the
ETL job produces wrong zone assignments, the ML model trains on corrupted features. This job
must also meet the < 5-minute SLA for 1.7 M rows — an important Spark optimisation exercise in
its own right. The output, `curated/porto-trips/` Parquet partitioned by `year_month`, is read by
both the ML feature engineering job and used to verify Zone KPIs in the Grafana analytics panel.

## Objective
Implement `spark_jobs/etl_porto.py` that reads the Porto CSV from MinIO, parses GPS polylines,
applies zone remapping, deduplicates, and writes a clean Parquet dataset partitioned by
`year_month` to `s3a://taasim/curated/porto-trips/` in under 5 minutes.

## Acceptance Criteria
- [ ] Spark job reads from `s3a://taasim/raw/porto-trips/train.csv`
- [ ] `POLYLINE` column parsed with `from_json` + `explode` → one row per GPS point
- [ ] Zone remapping applied: each GPS point enriched with `arrondissement_id` and `zone_type`
  (joined from `zone_mapping.csv` broadcast DataFrame)
- [ ] Deduplication: rows with `MISSING_DATA = True` dropped; trip-level deduplicate on `TRIP_ID`
- [ ] Output written as Parquet (snappy compression) to `s3a://taasim/curated/porto-trips/`
  partitioned by `year_month` (derived from `TIMESTAMP` field)
- [ ] Spark UI shows total job duration < 5 minutes on 1.7 M rows (screenshot captured)
- [ ] `spark.read.parquet("s3a://taasim/curated/porto-trips/")` can be re-read and schema is correct
- [ ] Row count before and after deduplication logged to console for audit

## Technical Hints
- Parse POLYLINE in PySpark:
  ```python
  from pyspark.sql.functions import from_json, explode, col
  from pyspark.sql.types import ArrayType, ArrayType, DoubleType

  polyline_schema = ArrayType(ArrayType(DoubleType()))
  df = df.withColumn("coords", from_json(col("POLYLINE"), polyline_schema))
  df = df.withColumn("coord", explode("coords"))
  df = df.withColumn("lon", col("coord")[0]).withColumn("lat", col("coord")[1])
  ```
- Broadcast the zone mapping DataFrame to avoid a shuffle join:
  ```python
  from pyspark.sql.functions import broadcast
  df = df.join(broadcast(zone_df), on="zone_id")
  ```
- Partitioning by `year_month` keeps ML reads fast (filter pushdown):
  ```python
  df.write.partitionBy("year_month") \
    .mode("overwrite") \
    .parquet("s3a://taasim/curated/porto-trips/")
  ```
- For performance: set `spark.sql.shuffle.partitions=8` on a local machine (default 200 is
  wasteful for this data size).
- Reference: project brief §9.4 Spark Jobs (ETL — Porto + NYC row).

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
Implemented in `spark_jobs/etl_porto.py`. Uses coordinate shifting (Porto→Casablanca) for zone mapping, broadcast join with zone_mapping.csv (16 rows), POLYLINE parsing via `from_json`+`explode`, deduplication on TRIP_ID, and Parquet output partitioned by year_month.
