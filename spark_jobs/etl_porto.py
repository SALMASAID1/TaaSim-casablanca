"""
spark_jobs/etl_porto.py — Spark ETL: Porto Taxi Trip Dataset
=============================================================

Sprint 4, Task 01  (v3 — optimised for single-worker cluster)

Purpose:
    Reads the raw Porto taxi CSV from MinIO (s3a://taasim/raw/porto-trips/train.csv),
    parses the POLYLINE JSON column to extract origin/destination GPS coordinates,
    maps Porto coordinates into the Casablanca frame via an ADR-01 affine bbox transform,
    assigns each trip to Casablanca arrondissements via a broadcast zone-mapping join,
    deduplicates trips, and writes the cleaned dataset as Parquet partitioned by year_month.

Performance optimisations (v3):
    1. Filter MISSING_DATA and deduplicate BEFORE parsing polylines (1.7M → ~1.5M)
    2. Extract origin + destination points per trip (first/last) without exploding all GPS points
       → keeps row count at 1.5M instead of exploding to 80M+
    3. Compute gps_point_count from the array SIZE() without exploding
    4. Zone mapping via broadcast bbox join; unmatched are explicitly tagged out_of_bounds
    5. Single cache materialization + one aggregation action for audit metrics/gates

Output:
    s3a://taasim/curated/porto-trips/   (Parquet, snappy, partitioned by year_month)

Run inside the Jupyter container (driver) connected to Spark master:
    spark-submit --master spark://spark-master:7077 /home/jovyan/spark_jobs/etl_porto.py
"""

import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    col,
    count as spark_count,
    date_format,
    element_at,
    from_json,
    from_unixtime,
    greatest,
    least,
    lit,
    size,
    sum as spark_sum,
    when,
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

# ADR-01 affine mapping: relative-position bbox transform (Porto → Casablanca)
# Source bbox (task04 hints): lon [-8.7, -8.5], lat [41.1, 41.2]
PORTO_BBOX = {"min_lon": -8.7, "max_lon": -8.5, "min_lat": 41.1, "max_lat": 41.2}

# Target bbox: union of metadata/zone_mapping.csv (maximizes zone join success)
CASA_BBOX = {"min_lon": -7.730, "max_lon": -7.480, "min_lat": 33.510, "max_lat": 33.645}

# Data-quality gates (fail fast if mapping is wrong)
MAX_OUT_OF_BOUNDS_RATE = 0.01  # 1%
MAX_CLAMP_RATE = 0.01          # 1% (warn above; raise once stable)

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


def affine_bbox_map(lon_col, lat_col, src_bbox, dst_bbox):
    """ADR-01: map (lon, lat) from src_bbox to dst_bbox via relative position.

    Returns: (dst_lon_col, dst_lat_col, clamped_bool_col)
    - Clamps relative coordinates to [0, 1] (matches Notebook 03 behavior)
    - Emits a boolean flag when clamping occurred (quality metric)
    """
    src_lon_span = float(src_bbox["max_lon"] - src_bbox["min_lon"])
    src_lat_span = float(src_bbox["max_lat"] - src_bbox["min_lat"])
    dst_lon_span = float(dst_bbox["max_lon"] - dst_bbox["min_lon"])
    dst_lat_span = float(dst_bbox["max_lat"] - dst_bbox["min_lat"])

    rel_lon_raw = (lon_col - lit(src_bbox["min_lon"])) / lit(src_lon_span)
    rel_lat_raw = (lat_col - lit(src_bbox["min_lat"])) / lit(src_lat_span)

    clamped = (
        (rel_lon_raw < lit(0.0)) | (rel_lon_raw > lit(1.0)) |
        (rel_lat_raw < lit(0.0)) | (rel_lat_raw > lit(1.0))
    )

    rel_lon = greatest(lit(0.0), least(lit(1.0), rel_lon_raw))
    rel_lat = greatest(lit(0.0), least(lit(1.0), rel_lat_raw))

    dst_lon = lit(dst_bbox["min_lon"]) + rel_lon * lit(dst_lon_span)
    dst_lat = lit(dst_bbox["min_lat"]) + rel_lat * lit(dst_lat_span)

    return dst_lon, dst_lat, clamped


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
        # STEP 3: Parse POLYLINE — extract ORIGIN + DESTINATION (no explode)
        #   - Instead of explode (1 trip → 50+ rows), we take first/last points
        #   - Also compute gps_point_count = SIZE(coords) for duration calc later
        #   - Apply ADR-01 affine bbox mapping (Porto → Casablanca)
        # ==================================================================
        logger.info("[STEP 3] Parsing POLYLINE — extracting origin/destination + point count ...")

        df = df.withColumn("coords", from_json(col("POLYLINE"), POLYLINE_SCHEMA))

        # Filter: coords must be non-null and have at least 1 point
        df = df.filter(col("coords").isNotNull() & (size("coords") > 0))

        # Extract raw origin/destination points (Porto coordinate space)
        last_pt = element_at(col("coords"), -1)
        df = (
            df
            .withColumn("origin_lon_raw", col("coords")[0][0])
            .withColumn("origin_lat_raw", col("coords")[0][1])
            .withColumn("dest_lon_raw", last_pt.getItem(0))
            .withColumn("dest_lat_raw", last_pt.getItem(1))
            .withColumn("gps_point_count", size("coords"))
        )

        # ADR-01 affine mapping: Porto bbox → Casablanca bbox (relative position)
        origin_lon, origin_lat, origin_clamped = affine_bbox_map(
            col("origin_lon_raw"), col("origin_lat_raw"), PORTO_BBOX, CASA_BBOX
        )
        dest_lon, dest_lat, dest_clamped = affine_bbox_map(
            col("dest_lon_raw"), col("dest_lat_raw"), PORTO_BBOX, CASA_BBOX
        )

        df = (
            df
            .withColumn("origin_lon", origin_lon)
            .withColumn("origin_lat", origin_lat)
            .withColumn("dest_lon", dest_lon)
            .withColumn("dest_lat", dest_lat)
            .withColumn("origin_clamped", origin_clamped)
            .withColumn("dest_clamped", dest_clamped)
            # Backward compatibility: representative trip point = origin
            .withColumn("lon", col("origin_lon"))
            .withColumn("lat", col("origin_lat"))
        )

        # Drop heavy and intermediate columns
        df = df.drop(
            "coords",
            "POLYLINE",
            "origin_lon_raw",
            "origin_lat_raw",
            "dest_lon_raw",
            "dest_lat_raw",
        )
        logger.info("  POLYLINE parsed — origin/destination mapped via affine bbox transform")

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
        # Range-based joins: origin and destination points matched to bbox zones

        zone_df_origin = zone_df.select(
            col("arrondissement_id").alias("origin_arrondissement_id"),
            col("zone_name").alias("origin_zone_name"),
            col("lon_min").alias("origin_lon_min"),
            col("lon_max").alias("origin_lon_max"),
            col("lat_min").alias("origin_lat_min"),
            col("lat_max").alias("origin_lat_max"),
        )

        df = df.join(
            broadcast(zone_df_origin),
            (col("origin_lon") >= col("origin_lon_min")) &
            (col("origin_lon") <= col("origin_lon_max")) &
            (col("origin_lat") >= col("origin_lat_min")) &
            (col("origin_lat") <= col("origin_lat_max")),
            "left",
        )

        df = (
            df
            .withColumn(
                "origin_zone_assignment_method",
                when(col("origin_arrondissement_id").isNotNull(), lit("bbox"))
                .otherwise(lit("out_of_bounds")),
            )
            .withColumn(
                "origin_arrondissement_id",
                when(col("origin_arrondissement_id").isNotNull(), col("origin_arrondissement_id"))
                .otherwise(lit(0)),
            )
            .withColumn(
                "origin_zone_name",
                when(col("origin_zone_name").isNotNull(), col("origin_zone_name"))
                .otherwise(lit("Out-of-bounds")),
            )
            # Backward compatibility: legacy zone fields = origin zone
            .withColumn("arrondissement_id", col("origin_arrondissement_id"))
            .withColumn("zone_name", col("origin_zone_name"))
            .withColumn("zone_assignment_method", col("origin_zone_assignment_method"))
        )

        df = df.drop("origin_lon_min", "origin_lon_max", "origin_lat_min", "origin_lat_max")

        zone_df_dest = zone_df.select(
            col("arrondissement_id").alias("dest_arrondissement_id"),
            col("zone_name").alias("dest_zone_name"),
            col("lon_min").alias("dest_lon_min"),
            col("lon_max").alias("dest_lon_max"),
            col("lat_min").alias("dest_lat_min"),
            col("lat_max").alias("dest_lat_max"),
        )

        df = df.join(
            broadcast(zone_df_dest),
            (col("dest_lon") >= col("dest_lon_min")) &
            (col("dest_lon") <= col("dest_lon_max")) &
            (col("dest_lat") >= col("dest_lat_min")) &
            (col("dest_lat") <= col("dest_lat_max")),
            "left",
        )

        df = (
            df
            .withColumn(
                "dest_zone_assignment_method",
                when(col("dest_arrondissement_id").isNotNull(), lit("bbox"))
                .otherwise(lit("out_of_bounds")),
            )
            .withColumn(
                "dest_arrondissement_id",
                when(col("dest_arrondissement_id").isNotNull(), col("dest_arrondissement_id"))
                .otherwise(lit(0)),
            )
            .withColumn(
                "dest_zone_name",
                when(col("dest_zone_name").isNotNull(), col("dest_zone_name"))
                .otherwise(lit("Out-of-bounds")),
            )
        )

        df = df.drop("dest_lon_min", "dest_lon_max", "dest_lat_min", "dest_lat_max")

        # Add zone_type
        df = df.withColumn(
            "zone_type",
            when(col("arrondissement_id") == 0, lit("out_of_bounds"))
            .when(col("arrondissement_id") <= 5, lit("commercial"))
            .when(col("arrondissement_id") <= 10, lit("residential"))
            .otherwise(lit("suburban"))
        )

        logger.info("  Zone remapping applied (broadcast joins: origin + destination)")

        # ==================================================================
        # STEP 6: Cache + audit metrics + data-quality gates
        #   - One aggregation action (materializes cache) for all metrics
        # ==================================================================
        logger.info("[STEP 6] Caching final DataFrame and computing audit metrics ...")
        df = df.cache()

        audit = (
            df.agg(
                spark_count(lit(1)).alias("total_trips"),
                spark_sum(when(col("origin_arrondissement_id") == 0, 1).otherwise(0)).alias("origin_out_of_bounds"),
                spark_sum(when(col("dest_arrondissement_id") == 0, 1).otherwise(0)).alias("dest_out_of_bounds"),
                spark_sum(col("origin_clamped").cast("int")).alias("origin_clamped"),
                spark_sum(col("dest_clamped").cast("int")).alias("dest_clamped"),
            )
            .collect()[0]
        )

        total_trips = int(audit["total_trips"])
        origin_oob = int(audit["origin_out_of_bounds"])
        dest_oob = int(audit["dest_out_of_bounds"])
        origin_clamped = int(audit["origin_clamped"])
        dest_clamped = int(audit["dest_clamped"])

        origin_oob_rate = (origin_oob / total_trips) if total_trips else 0.0
        dest_oob_rate = (dest_oob / total_trips) if total_trips else 0.0
        origin_clamp_rate = (origin_clamped / total_trips) if total_trips else 0.0
        dest_clamp_rate = (dest_clamped / total_trips) if total_trips else 0.0

        logger.info("  ✅ Total trips: %d", total_trips)
        logger.info("  Origin out_of_bounds: %d (%.3f%%)", origin_oob, origin_oob_rate * 100)
        logger.info("  Dest   out_of_bounds: %d (%.3f%%)", dest_oob, dest_oob_rate * 100)
        logger.info("  Origin clamped: %d (%.3f%%)", origin_clamped, origin_clamp_rate * 100)
        logger.info("  Dest   clamped: %d (%.3f%%)", dest_clamped, dest_clamp_rate * 100)

        if origin_clamp_rate > MAX_CLAMP_RATE or dest_clamp_rate > MAX_CLAMP_RATE:
            logger.warning(
                "  ⚠️ Clamp rate above threshold (max=%.3f%%) — check PORTO_BBOX/CASA_BBOX assumptions",
                MAX_CLAMP_RATE * 100,
            )

        if origin_oob_rate > MAX_OUT_OF_BOUNDS_RATE or dest_oob_rate > MAX_OUT_OF_BOUNDS_RATE:
            raise RuntimeError(
                "Zone join out_of_bounds rate too high: "
                f"origin={origin_oob_rate:.4%}, dest={dest_oob_rate:.4%} (max={MAX_OUT_OF_BOUNDS_RATE:.4%}). "
                "Check affine bbox mapping and zone_mapping.csv bbox coverage."
            )

        # Audit-only columns are not needed in the curated output
        df = df.drop("origin_clamped", "dest_clamped")

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
