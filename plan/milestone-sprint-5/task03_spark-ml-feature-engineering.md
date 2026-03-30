# task03 — Spark ML Feature Engineering

## Context
The GBT demand forecasting model is only as good as the features fed to it. This job transforms
the cleaned Porto trip data into a structured feature matrix that the model can consume directly.
Feature engineering is the most intellectually demanding Spark task in the project: it requires
window functions for lag features, API calls for weather data, and a zone reference join for
spatial features. The output `ml/features/` is the direct input to the model training job.

## Objective
Implement `spark_jobs/ml_feature_engineering.py` that reads `curated/porto-trips/`, computes
all four feature groups (temporal, spatial, weather, lag), and writes the feature matrix to
`s3a://taasim/ml/features/`.

## Acceptance Criteria
- [ ] Feature matrix produced with the following columns:
  - **Temporal**: `hour_of_day`, `day_of_week`, `is_weekend` (bool), `is_friday` (bool)
  - **Spatial**: `zone_id`, `zone_population_density`, `zone_type` (one-hot encoded:
    `zone_type_residential`, `zone_type_commercial`, `zone_type_transit_hub`)
  - **Weather**: `is_raining` (bool), `temperature_bucket` (0=cold <15°C / 1=mild / 2=hot >28°C)
  - **Lag**: `demand_lag_1d`, `demand_lag_7d`, `rolling_7d_mean`
  - **Target**: `trip_count` (number of trip requests per zone per 30-min slot)
- [ ] Lag features computed using Spark `Window` functions partitioned by `zone_id`, ordered by
  `time_slot_start`
- [ ] Weather data joined from Open-Meteo historical API (Porto coordinates, July 2013–June 2014)
- [ ] Zone reference table joined: `zone_population_density` and `zone_type` from `zone_mapping.csv`
- [ ] Feature matrix written as Parquet to `s3a://taasim/ml/features/`
- [ ] Feature matrix row count = (16 zones × 365 days × 48 half-hour slots) ≈ 280,320 rows
  (allow ±10% for missing weather data)
- [ ] No null values in any feature column (null-imputed before writing)

## Technical Hints
- Aggregate Porto trips to (zone_id, time_slot_30min) → trip_count first, then compute features:
  ```python
  from pyspark.sql.functions import window, count
  agg_df = trips_df.groupBy(
      col("arrondissement_id").alias("zone_id"),
      window("event_time", "30 minutes").alias("slot")
  ).agg(count("*").alias("trip_count"))
  ```
- Lag features with Window:
  ```python
  from pyspark.sql.window import Window
  from pyspark.sql.functions import lag, avg

  w = Window.partitionBy("zone_id").orderBy("slot_start")
  df = df.withColumn("demand_lag_1d", lag("trip_count", 48).over(w))   # 48 slots = 1 day
  df = df.withColumn("demand_lag_7d", lag("trip_count", 48*7).over(w))
  df = df.withColumn("rolling_7d_mean",
      avg("trip_count").over(w.rowsBetween(-48*7, -1)))
  ```
- Open-Meteo historical API (free, no key):
  `https://archive-api.open-meteo.com/v1/archive?latitude=41.15&longitude=-8.61&start_date=2013-07-01&end_date=2014-06-30&hourly=temperature_2m,precipitation`
  Fetch once, save as CSV to MinIO, join in Spark.
- Impute nulls for lag columns (first 7 days have no 7-day lag): use `fillna(0)`.
- Reference: project brief §5.2 Feature Set, §9.4 Spark Jobs (ML — Demand Forecasting).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
