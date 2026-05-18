"""
spark_jobs/etl_nyc_tlc.py — Spark ETL: Logical NYC-to-Casablanca Projection
==========================================================================

Sprint 4, Task 02 (Logical Spatial Projection & Quality Enforcement)

Strategy:
    1. Define a tight Urban Core for Casablanca to avoid the Atlantic Ocean.
    2. Use the original NYC 'trip_distance' as a constraint.
    3. Remove 'illogical' trips (Out of bounds, Speed > 120km/h, Zero distance).
    4. Calculate the Dropoff using the 'Destination Point' formula (Spherical Trig).
    5. COASTLINE FILTER: Use a linear function to ensure no trips are on the beach or in water.
"""

import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, count, avg, broadcast, lit, expr, when,
    rand, asin, sin, cos, atan2, sqrt, pow, radians, degrees, floor, concat_ws,
    unix_timestamp
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etl_nyc_tlc")

# ---------------------------------------------------------------------------
# Constants & Urban Boundaries
# ---------------------------------------------------------------------------
RAW_PATH       = "s3a://taasim/raw/nyc-tlc/yellow_tripdata_2019-01.parquet"
ML_OUTPUT_PATH = "s3a://taasim/curated/nyc-ml-features/"

# Refined Urban Core (Avoids Ocean and Suburbs)
MIN_LON, MAX_LON = -7.68, -7.58
MIN_LAT, MAX_LAT = 33.52, 33.60
EARTH_RADIUS_KM = 6371.0
GRID_SIZE = 20

def create_spark_session():
    return (
        SparkSession.builder
        .appName("TaaSim — Logical NYC Projection")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

def remove_illogical_trips(df):
    """
    Quality enforcement layer to remove noise and data errors.
    """
    logger.info("Applying Quality Enforcement (Removing Illogical Trips)...")
    
    # 1. Basic cleaning: positive metrics
    df = df.filter((col("trip_distance") > 0.1) & (col("fare_amount") > 2.5))
    
    # 2. Time-based logic: Trip must have a valid duration
    if "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn("duration_sec", 
            unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")
        )
        df = df.filter((col("duration_sec") >= 60) & (col("duration_sec") <= 10800))
        
        # 3. Speed logic: Average speed max 100km/h
        df = df.withColumn("avg_speed_kmh", 
            (col("trip_distance") * 1.60934) / (col("duration_sec") / 3600)
        )
        df = df.filter(col("avg_speed_kmh") <= 100)
        
    return df

def apply_logical_projection(df):
    """
    Implements the 'Destination Point' formula and the Linear Coastline Mask.
    """
    logger.info("Applying Logical Distance-Preserving Projection...")
    
    # 1. Convert Miles to KM
    df = df.withColumn("orig_dist_km", col("trip_distance") * 1.60934)
    
    # 2. Derive deterministic start/end zones from PULocationID/DOLocationID
    # This ensures trips from the same NYC zone map to the same Casablanca area.
    df = df.withColumn("start_x", col("PULocationID") % lit(GRID_SIZE)) \
           .withColumn("start_y", (col("PULocationID") / lit(GRID_SIZE)).cast("int") % lit(GRID_SIZE)) \
           .withColumn("target_x", col("DOLocationID") % lit(GRID_SIZE)) \
           .withColumn("target_y", (col("DOLocationID") / lit(GRID_SIZE)).cast("int") % lit(GRID_SIZE))
    
    # 3. Pick a semi-random Pickup inside the mapped Start Zone
    cell_width = (MAX_LON - MIN_LON) / GRID_SIZE
    cell_height = (MAX_LAT - MIN_LAT) / GRID_SIZE
    
    df = df.withColumn("pickup_lon", lit(MIN_LON) + (col("start_x") + rand()) * lit(cell_width)) \
           .withColumn("pickup_lat", lit(MIN_LAT) + (col("start_y") + rand()) * lit(cell_height))
    
    # 4. Generate bearing based on the vector from Start Zone to Target Zone
    # If Start == Target (intra-zone), fallback to random bearing
    df = df.withColumn("bearing", when(
        col("PULocationID") != col("DOLocationID"),
        atan2(col("target_y") - col("start_y"), col("target_x") - col("start_x"))
    ).otherwise(rand() * 2 * 3.1415926535))
    
    # 5. Math: Spherical Trig to find Dropoff Coordinate using original distance
    lat1 = radians(col("pickup_lat"))
    lon1 = radians(col("pickup_lon"))
    d_r = col("orig_dist_km") / lit(EARTH_RADIUS_KM)
    
    df = df.withColumn("dropoff_lat_rad", asin(
        sin(lat1) * cos(d_r) + cos(lat1) * sin(d_r) * cos(col("bearing"))
    ))
    
    df = df.withColumn("dropoff_lon_rad", lon1 + atan2(
        sin(col("bearing")) * sin(d_r) * cos(lat1),
        cos(d_r) - sin(lat1) * sin(col("dropoff_lat_rad"))
    ))
    
    # Convert back to degrees
    df = df.withColumn("dropoff_lat", degrees(col("dropoff_lat_rad"))) \
           .withColumn("dropoff_lon", degrees(col("dropoff_lon_rad")))
    
    # 5. COASTLINE FILTER: Discard trips on the beach or in water
    # Logic: Casablanca coastline follows Lat = 0.3 * Lon + 35.88
    # Any trip ABOVE this line is in the water/sand.
    df = df.filter(col("dropoff_lat") < (col("dropoff_lon") * 0.3 + 35.88))
    df = df.filter(col("pickup_lat") < (col("pickup_lon") * 0.3 + 35.88))
    
    # Ensure it's within a reasonable long range
    df = df.filter((col("dropoff_lon") >= -7.75) & (col("dropoff_lon") <= -7.45))
    
    # 6. Grid Zoning (20x20)
    cell_width = (MAX_LON - MIN_LON) / GRID_SIZE
    cell_height = (MAX_LAT - MIN_LAT) / GRID_SIZE
    
    df = df.withColumn("grid_x", floor((col("pickup_lon") - lit(MIN_LON)) / lit(cell_width)).cast("int")) \
           .withColumn("grid_y", floor((col("pickup_lat") - lit(MIN_LAT)) / lit(cell_height)).cast("int")) \
           .withColumn("pickup_zone_id", concat_ws("_", col("grid_x"), col("grid_y")))
    
    return df

def main():
    spark = create_spark_session()
    try:
        logger.info("Reading raw data...")
        df = spark.read.parquet(RAW_PATH).select(
            "tpep_pickup_datetime", "tpep_dropoff_datetime", 
            "trip_distance", "fare_amount", "passenger_count",
            "PULocationID", "DOLocationID"
        )
        
        # 1. Quality Enforcement
        df = remove_illogical_trips(df)
        
        # 2. Apply Spatial Logic
        df = apply_logical_projection(df)
        
        # 3. Feature Extraction
        df = df.withColumn("hour_of_day", hour(col("tpep_pickup_datetime"))) \
               .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
        
        logger.info("Writing curated ML features...")
        df.write.mode("overwrite").parquet(ML_OUTPUT_PATH)
        
        logger.info("ETL Success. Total Clean Urban Trips: %d", df.count())
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
