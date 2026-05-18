# task02 — Spark ETL: NYC TLC Dataset (Casablanca ML Projection)

## Context
The NYC TLC dataset (Yellow Taxi Parquet) is TaaSim's large-scale Spark exercise. It is used to simulate a realistic mobility data stream for Casablanca by projecting NYC metrics onto Casablanca's physical geography. This dataset provides rich features for ML training (e.g., forecasting models) and hourly demand aggregates for Grafana analytics. Processing this cleanly within time and memory budget while handling complex mathematical spatial projections (Haversine distance, grid mapping) is the primary deliverable.

## Objective
Implement `spark_jobs/etl_nyc_tlc.py` that reads 1 month of NYC TLC Parquet from MinIO (specifically `yellow_tripdata_2019-01.parquet` to ensure it is manageable for ML training), applies a synthetic geographical projection to Casablanca, computes Haversine distances, and generates two outputs:
1. `s3a://taasim/curated/nyc-ml-features/`: Detailed trip data for Machine Learning.
2. `s3a://taasim/curated/demand-by-zone/`: Per-zone-per-hour demand aggregates.

## Acceptance Criteria
- [ ] Spark job reads ONE Parquet file: `s3a://taasim/raw/nyc-tlc/yellow_tripdata_2019-01.parquet`
- [ ] Projection applied purely in PySpark functions:
  - Unit conversion: Miles to Kilometers.
  - Synthetic Coordinates: Randomly generated `pickup_lat`, `pickup_lon`, `dropoff_lat`, `dropoff_lon` within Casablanca Bounding Box.
  - Haversine Filter: Calculate Great-Circle distance natively in PySpark and filter trips between `0.5km` and `20km`.
  - Grid Mapping: 20x20 grid string assignment (`pickup_zone_id`).
- [ ] Per-zone-per-hour demand aggregates computed and written to `s3a://taasim/curated/demand-by-zone/`.
- [ ] Detailed ML features written to `s3a://taasim/curated/nyc-ml-features/`.
- [ ] At least one broadcast join used (for the zone reference table) to keep existing integrations intact.
- [ ] Job processes the file without OOM errors on a single Spark worker (4 GB RAM).

## Technical Hints
- To calculate Haversine in PySpark, use math functions from `pyspark.sql.functions`: `asin`, `sqrt`, `pow`, `sin`, `cos`, `radians`.
- Generate random coordinates using `rand()`. Casablanca BBox approx: Lon `[-7.75, -7.50]`, Lat `[33.50, 33.65]`.
- Aggregation should now include the new ML metrics:
  ```python
  agg_df = df.groupBy("pickup_zone_id", "arrondissement_id", "hour_of_day", "day_of_week") \
             .agg(count("*").alias("trip_count"),
                  avg("haversine_distance_km").alias("avg_distance_km"),
                  avg("fare_amount").alias("avg_fare"))
  ```
- Reference: project brief §2.2 NYC TLC Trip Records, §9.4 Spark Jobs.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
Adapted to utilize the advanced Casablana projection logic explored during data EDA. Limits processing to 1 file for ML training efficiency.
