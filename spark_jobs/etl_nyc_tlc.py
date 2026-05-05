"""
spark_jobs/etl_nyc_tlc.py — Spark ETL: NYC TLC Yellow Taxi Dataset
===================================================================

Sprint 4, Task 02

Purpose:
    Reads 3 months of NYC TLC Yellow Taxi Parquet data from MinIO,
    computes per-zone-per-hour demand aggregates, and writes the results
    to s3a://taasim/curated/demand-by-zone/.

    This is TaaSim's large-scale Spark exercise: ~30M rows across 3 months.
    The focus is on real Spark optimisation: partitioning, broadcast joins,
    and columnar Parquet reads.

Output:
    s3a://taasim/curated/demand-by-zone/   (Parquet, snappy)

    Schema:
        PULocationID     int        — NYC pickup zone (maps to Casablanca arrondissement)
        hour_of_day      int        — Hour of pickup (0-23)
        day_of_week      int        — Day of week (1=Sunday ... 7=Saturday)
        trip_count        long       — Number of trips
        avg_distance     double     — Average trip distance (miles)
        avg_fare         double     — Average fare amount ($)
        arrondissement_id int       — Mapped Casablanca zone
        zone_name        text       — Casablanca zone name

Run inside the Jupyter container:
    spark-submit --master spark://spark-master:7077 spark_jobs/etl_nyc_tlc.py
"""

import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, count, avg, broadcast, lit, expr
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etl_nyc_tlc")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAW_PATH       = "s3a://taasim/raw/nyc-tlc/*.parquet"
ZONE_MAP_PATH  = "s3a://taasim/metadata/zone_mapping.csv"
OUTPUT_PATH    = "s3a://taasim/curated/demand-by-zone/"


def create_spark_session():
    """Create a SparkSession optimised for the NYC TLC workload."""
    return (
        SparkSession.builder
        .appName("TaaSim — ETL NYC TLC Demand Aggregates")
        .master("spark://spark-master:7077")
        # --- Performance tuning ---
        # 16 shuffle partitions (not default 200) for local cluster
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "8")
        # Memory: ensure worker stays within 4GB budget
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        # --- S3A / MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def read_raw_data(spark):
    """
    Read 3 months of NYC TLC Yellow Taxi Parquet from MinIO.

    Uses wildcard path to read all .parquet files in one call.
    Parquet columnar format allows Spark to push down column selection,
    only reading the 5 columns we need (not all 18).
    """
    logger.info("Reading NYC TLC Parquet from %s ...", RAW_PATH)

    df = spark.read.parquet(RAW_PATH)

    total_rows = df.count()
    logger.info("Total rows loaded: %d (~%.1f M)", total_rows, total_rows / 1_000_000)
    logger.info("Schema:")
    df.printSchema()

    return df, total_rows


def select_and_filter(df):
    """
    Select only the relevant columns and filter invalid records.

    Columns used (from task spec):
        - tpep_pickup_datetime: timestamp of pickup
        - PULocationID:         pickup location zone ID
        - trip_distance:        trip distance in miles
        - fare_amount:          fare in dollars
        - passenger_count:      number of passengers

    Filters:
        - Non-null pickup datetime
        - trip_distance > 0
        - fare_amount > 0
        - PULocationID is not null
    """
    logger.info("Selecting relevant columns and filtering invalid records ...")

    df = df.select(
        "tpep_pickup_datetime",
        "PULocationID",
        "trip_distance",
        "fare_amount",
        "passenger_count"
    )

    # Filter out invalid rows
    df_clean = (
        df
        .filter(col("tpep_pickup_datetime").isNotNull())
        .filter(col("PULocationID").isNotNull())
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") > 0)
    )

    clean_count = df_clean.count()
    logger.info("Rows after filtering: %d", clean_count)

    return df_clean


def extract_time_features(df):
    """
    Extract temporal features from the pickup datetime:
        - hour_of_day (0-23): for hourly demand analysis
        - day_of_week (1-7):  for weekly pattern analysis
    """
    logger.info("Extracting hour_of_day and day_of_week ...")

    df = (
        df
        .withColumn("hour_of_day", hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
    )

    return df


def apply_zone_mapping(df, spark):
    """
    Map NYC PULocationID to Casablanca arrondissements via broadcast join.

    NYC uses PULocationID (1-263), Casablanca has 16 arrondissements (1-16).
    We map using modular arithmetic: arrondissement_id = (PULocationID % 16) + 1

    This demonstrates the broadcast join pattern required by the acceptance criteria:
    the 16-row zone reference table is broadcast to all executors.
    """
    logger.info("Loading zone mapping from %s ...", ZONE_MAP_PATH)

    zone_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(ZONE_MAP_PATH)
    )
    logger.info("Zone mapping loaded — %d zones", zone_df.count())

    # Create the mapping column: PULocationID → arrondissement_id
    df = df.withColumn(
        "mapped_zone_id",
        (col("PULocationID") % 16) + 1
    )

    # ---- BROADCAST JOIN ----
    # The zone table (16 rows) is broadcast to every executor.
    # This avoids a shuffle join on the ~30M row dataset.
    # (Acceptance criteria: "At least one broadcast join used — documented in code comments")
    logger.info("Applying broadcast join with zone reference table ...")
    df_zoned = df.join(
        broadcast(zone_df),
        df["mapped_zone_id"] == zone_df["arrondissement_id"],
        "left"
    )

    # Clean up: drop intermediate and bounding-box columns
    df_zoned = df_zoned.drop(
        "mapped_zone_id", "lon_min", "lon_max", "lat_min", "lat_max"
    )

    logger.info("Broadcast zone mapping complete")
    return df_zoned


def compute_demand_aggregates(df):
    """
    Compute per-zone-per-hour demand aggregates.

    Grouping key: (PULocationID, hour_of_day, day_of_week)

    Metrics:
        - trip_count:    total number of trips
        - avg_distance:  average trip distance in miles
        - avg_fare:      average fare amount in dollars
    """
    logger.info("Computing demand aggregates ...")

    agg_df = (
        df.groupBy("PULocationID", "hour_of_day", "day_of_week",
                    "arrondissement_id", "zone_name")
        .agg(
            count("*").alias("trip_count"),
            avg("trip_distance").alias("avg_distance"),
            avg("fare_amount").alias("avg_fare")
        )
    )

    agg_count = agg_df.count()
    logger.info("Aggregate rows: %d", agg_count)

    return agg_df


def write_parquet(df):
    """Write the aggregated demand data as Parquet to MinIO."""
    logger.info("Writing demand aggregates to %s ...", OUTPUT_PATH)

    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(OUTPUT_PATH)
    )

    logger.info("Parquet write complete")


def validate_output(spark):
    """Re-read output and validate."""
    logger.info("Validating output ...")
    df = spark.read.parquet(OUTPUT_PATH)
    df.printSchema()
    row_count = df.count()
    logger.info("Output row count: %d", row_count)
    logger.info("Sample rows:")
    df.show(10, truncate=False)
    return row_count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 70)
    logger.info("TaaSim ETL — NYC TLC Yellow Taxi Dataset")
    logger.info("=" * 70)

    start_time = time.time()
    spark = create_spark_session()
    logger.info("SparkSession created — app: %s", spark.sparkContext.appName)

    try:
        # 1. Read raw Parquet (3 months, ~30M rows)
        df, total_rows = read_raw_data(spark)

        # 2. Select relevant columns + filter invalid records
        df = select_and_filter(df)

        # 3. Extract temporal features
        df = extract_time_features(df)

        # 4. Apply zone mapping (broadcast join)
        df = apply_zone_mapping(df, spark)

        # 5. Compute per-zone-per-hour aggregates
        agg_df = compute_demand_aggregates(df)

        # 6. Write Parquet
        write_parquet(agg_df)

        # 7. Validate output
        validate_output(spark)

    finally:
        spark.stop()

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("ETL complete in %.1f seconds (%.1f minutes)", elapsed, elapsed / 60)
    logger.info("Total input: %d rows (~%.1f M)", total_rows, total_rows / 1_000_000)
    logger.info("No stage should exceed 3 minutes — verify in Spark UI at :8080")
    logger.info("Spark UI screenshots → docs/spark-nyc-ui.png")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
