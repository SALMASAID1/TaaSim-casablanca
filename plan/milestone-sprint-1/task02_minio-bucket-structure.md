# task02 — MinIO Bucket Structure

## Context
MinIO acts as TaaSim's distributed data lake, equivalent to AWS S3 in the production architecture.
Every Spark job, Kafka Connect sink, and ML artifact flows through MinIO. Establishing the bucket
layout in Sprint 1 means all subsequent tasks can reference stable, documented paths without
rework.

## Objective
Create and document the four-zone MinIO bucket structure (`raw/`, `curated/`, `ml/`,
`raw/kafka-archive/`) and verify that each prefix is writable from both the host and from a
container running a PySpark shell.

## Acceptance Criteria
- [x] Bucket `taasim` created with the following prefixes present: `raw/porto-trips/`,
  `raw/nyc-tlc/`, `raw/kafka-archive/`, `curated/trips/`, `curated/demand-by-zone/`,
  `ml/features/`, `ml/models/demand_v1/`
- [x] Porto CSV files uploaded to `raw/porto-trips/` (at least a 10 000-row sample for early testing)
- [x] NYC TLC Parquet for 1 month uploaded to `raw/nyc-tlc/`
- [x] `mc ls local/taasim --recursive` lists all prefixes
- [x] PySpark snippet `spark.read.csv("s3a://taasim/raw/porto-trips/")` succeeds inside a Spark
  container shell (confirms S3A connector wired correctly)
- [x] Bucket structure diagram committed to `docs/sprint-1/minio-layout.md`

## Technical Hints
- Create bucket and prefixes with MinIO Client:
  ```bash
  mc mb local/taasim
  mc cp train.csv local/taasim/raw/porto-trips/train.csv
  ```
- Porto dataset download: https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i
  (free Kaggle account required). File is `train.csv` (~1.5 GB compressed).
- NYC TLC Parquet: direct download (no login) from
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page — pick 3 consecutive months
  of Yellow Taxi data.
- S3A configuration in `spark-defaults.conf` or as SparkConf:
  ```
  spark.hadoop.fs.s3a.endpoint=http://minio:9000
  spark.hadoop.fs.s3a.access.key=minioadmin
  spark.hadoop.fs.s3a.secret.key=minioadmin
  spark.hadoop.fs.s3a.path.style.access=true
  spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  ```
- Reference: project brief §4.2 MinIO Bucket Structure.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
