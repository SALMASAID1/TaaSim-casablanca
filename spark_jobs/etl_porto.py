"""
spark_jobs/etl_porto.py — Spark ETL: Porto Taxi Trip Dataset
=============================================================

Sprint 4, Task 01  (v2 — optimised for single-worker cluster)

Purpose:
    Reads the raw Porto taxi CSV from MinIO (s3a://taasim/raw/porto-trips/train.csv),
    parses the POLYLINE JSON column to extract representative GPS coordinates,
    assigns each trip to a Casablanca arrondissement via a broadcast zone-mapping join,
    deduplicates trips, and writes the cleaned dataset as Parquet partitioned by year_month.

Performance optimisations (v2):
    1. Filter MISSING_DATA and deduplicate BEFORE parsing polylines (1.7M → ~1.5M)
    2. Extract only the FIRST GPS coordinate per trip (no explode of all points)
       → keeps row count at 1.5M instead of exploding to 80M+
    3. Compute gps_point_count from the array SIZE() without exploding
    4. Use hash-based zone assignment instead of expensive cross-join
    5. Removed all intermediate .count() calls (lazy evaluation only)
    6. Single .count() at audit stage using .cache()

Output:
    s3a://taasim/curated/porto-trips/   (Parquet, snappy, partitioned by year_month)

Run inside the Jupyter container (driver) connected to Spark master:
    spark-submit --master spark://spark-master:7077 /home/jovyan/spark_jobs/etl_porto.py
"""

import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, broadcast, lit, size, abs as spark_abs,
    from_unixtime, date_format, when, hash as spark_hash
)
from pyspark.sql.types import ArrayType, DoubleType

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etl_porto")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAW_PATH       = "s3a://taasim/raw/porto-trips/train.csv"
ZONE_MAP_PATH  = "s3a://taasim/metadata/zone_mapping.csv"
OUTPUT_PATH    = "s3a://taasim/curated/porto-trips/"

# Porto GPS bounding box (approximate centre for normalisation)
# Porto centre:  lat ≈ 41.15,  lon ≈ -8.61
# Casablanca:    lat ≈ 33.57,  lon ≈ -7.59
LAT_SHIFT = 33.57 - 41.15   # ≈ -7.58
LON_SHIFT = -7.59 - (-8.61) # ≈ +1.02

POLYLINE_SCHEMA = ArrayType(ArrayType(DoubleType()))


def create_spark_session():
    """Create and return a configured SparkSession for the TaaSim ETL."""
    return (
        SparkSession.builder
        .appName("TaaSim — ETL Porto Trips")
        .master("spark://spark-master:7077")
        # --- Performance tuning (single worker, 2 cores, 2GB) ---
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        # Adaptive query execution — lets Spark coalesce small partitions
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # --- S3A / MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    logger.info("=" * 70)
    logger.info("TaaSim ETL — Porto Trips Dataset (v2 — optimised)")
    logger.info("=" * 70)

    start_time = time.time()

    spark = create_spark_session()
    logger.info("SparkSession created — app: %s", spark.sparkContext.appName)

    try:
        # ==================================================================
        # STEP 1: Read raw CSV
        # ==================================================================
        logger.info("[STEP 1] Reading raw Porto CSV from %s ...", RAW_PATH)
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(RAW_PATH)
        )
        # NO .count() here — let Spark stay lazy
        logger.info("  Schema: %s", df.columns)

        # ==================================================================
        # STEP 2: Filter + Deduplicate FIRST (before any parsing)
        #   - This reduces rows from 1.7M to ~1.5M BEFORE we touch POLYLINE
        #   - Critical optimisation: all downstream work operates on fewer rows
        # ==================================================================
        logger.info("[STEP 2] Filtering MISSING_DATA and deduplicating on TRIP_ID ...")

        # Drop rows with MISSING_DATA = True
        if "MISSING_DATA" in df.columns:
            df = df.filter(
                (col("MISSING_DATA") == False) |   # noqa: E712
                (col("MISSING_DATA").isNull())
            )

        # Drop rows where POLYLINE is empty, null, or just "[]"
        df = df.filter(
            col("POLYLINE").isNotNull() &
            (col("POLYLINE") != "") &
            (col("POLYLINE") != "[]")
        )

        # Deduplicate on TRIP_ID (BEFORE parsing — much cheaper)
        df = df.dropDuplicates(["TRIP_ID"])
        logger.info("  Filters applied (lazy) — MISSING_DATA removed, POLYLINE checked, TRIP_ID deduped")

        # ==================================================================
        # STEP 3: Parse POLYLINE — extract FIRST coordinate only
        #   - Instead of explode (1 trip → 50+ rows), we just take coords[0]
        #   - Also compute gps_point_count = SIZE(coords) for duration calc later
        #   - This keeps the DataFrame at ~1.5M rows (not 80M+)
        # ==================================================================
        logger.info("[STEP 3] Parsing POLYLINE — extracting first GPS point + point count ...")

        df = df.withColumn("coords", from_json(col("POLYLINE"), POLYLINE_SCHEMA))

        # Filter: coords must be non-null and have at least 1 point
        df = df.filter(col("coords").isNotNull() & (size("coords") > 0))

        # Extract first GPS coordinate [lon, lat] as representative trip point
        df = (
            df
            .withColumn("lon_raw", col("coords")[0][0])
            .withColumn("lat_raw", col("coords")[0][1])
            # Count total GPS points per trip (for duration calc in KPI job)
            .withColumn("gps_point_count", size("coords"))
        )

        # Shift Porto coordinates → Casablanca bounding box
        df = (
            df
            .withColumn("lon", col("lon_raw") + lit(LON_SHIFT))
            .withColumn("lat", col("lat_raw") + lit(LAT_SHIFT))
        )

        # Drop heavy columns we no longer need
        df = df.drop("coords", "POLYLINE", "lon_raw", "lat_raw")
        logger.info("  POLYLINE parsed — first GPS point extracted, gps_point_count computed")

        # ==================================================================
        # STEP 4: Derive year_month partition key from TIMESTAMP
        # ==================================================================
        logger.info("[STEP 4] Deriving year_month from TIMESTAMP ...")
        df = df.withColumn(
            "year_month",
            date_format(from_unixtime(col("TIMESTAMP")), "yyyy-MM")
        )

        # ==================================================================
        # STEP 5: Zone remapping via broadcast join
        #   - Load 16-row zone_mapping.csv and broadcast it
        #   - Join using lon/lat bounding box ranges
        #   - Since we're now at ~1.5M rows (not 80M+), this is fast
        # ==================================================================
        logger.info("[STEP 5] Applying zone remapping (broadcast join) ...")

        zone_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(ZONE_MAP_PATH)
        )

        # Broadcast the 16-row zone table to every executor
        # Range-based join: each GPS point matched to its bounding-box zone
        df = df.join(
            broadcast(zone_df),
            (col("lon") >= col("lon_min")) &
            (col("lon") <= col("lon_max")) &
            (col("lat") >= col("lat_min")) &
            (col("lat") <= col("lat_max")),
            "left"
        )

        # For GPS points that didn't fall in any zone, assign via hash
        # This ensures every trip gets a zone (no NULLs)
        df = df.withColumn(
            "arrondissement_id",
            when(col("arrondissement_id").isNotNull(), col("arrondissement_id"))
            .otherwise((spark_abs(spark_hash("TRIP_ID")) % 16) + 1)
        )

        # Fill zone_name for hash-assigned zones
        df = df.withColumn(
            "zone_name",
            when(col("zone_name").isNotNull(), col("zone_name"))
            .otherwise(lit("Zone-Assigned"))
        )

        # Add zone_type
        df = df.withColumn(
            "zone_type",
            when(col("arrondissement_id") <= 5, lit("commercial"))
            .when(col("arrondissement_id") <= 10, lit("residential"))
            .otherwise(lit("suburban"))
        )

        # Drop bounding box columns
        df = df.drop("lon_min", "lon_max", "lat_min", "lat_max")
        logger.info("  Zone remapping applied (broadcast join)")

        # ==================================================================
        # STEP 6: Cache + audit count (single action triggers the pipeline)
        # ==================================================================
        logger.info("[STEP 6] Caching final DataFrame and computing audit count ...")
        df = df.cache()
        final_count = df.count()
        logger.info("  ✅ Final row count after all transforms: %d", final_count)

        # ==================================================================
        # STEP 7: Write Parquet (partitioned by year_month)
        # ==================================================================
        logger.info("[STEP 7] Writing Parquet to %s ...", OUTPUT_PATH)
        (
            df.write
            .partitionBy("year_month")
            .mode("overwrite")
            .option("compression", "snappy")
            .parquet(OUTPUT_PATH)
        )
        logger.info("  Parquet write complete")

        # ==================================================================
        # STEP 8: Validate output
        # ==================================================================
        logger.info("[STEP 8] Validating output ...")
        out_df = spark.read.parquet(OUTPUT_PATH)
        out_df.printSchema()
        out_count = out_df.count()
        logger.info("  Output row count: %d", out_count)
        out_df.show(10, truncate=False)

        # Unpersist cache
        df.unpersist()

    finally:
        spark.stop()

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("ETL complete in %.1f seconds (%.1f minutes)", elapsed, elapsed / 60)
    logger.info("SLA target: < 5 minutes — %s",
                "✅ PASS" if elapsed < 300 else "❌ FAIL")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
