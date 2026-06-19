"""
spark_jobs/ml_model_training.py — GBT Model Training & Validation
=============================================================

Sprint 5, Task 04
"""

import time
import logging
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, abs, sqrt, mean
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ml_model_training")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FEATURES_PATH = "s3a://taasim/ml/features/"
MODEL_OUTPUT_PATH = "s3a://taasim/ml/models/demand_v1/"
IMPORTANCE_OUTPUT_PATH = "s3a://taasim/ml/models/demand_v1/feature_importances_out"

TABLE_OUTPUT = "/home/jovyan/spark_jobs/ml-evaluation-table.md"
CHART_OUTPUT = "/home/jovyan/spark_jobs/ml-feature-importance.png"

def create_spark_session():
    return (
        SparkSession.builder
        .appName("TaaSim — ML Model Training")
        .master("spark://spark-master:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    logger.info("=" * 70)
    logger.info("TaaSim ML — GBT Model Training")
    logger.info("=" * 70)

    start_time = time.time()
    spark = create_spark_session()

    try:
        # ==================================================================
        # STEP 1: Load Data & Split
        # ==================================================================
        logger.info("Loading feature matrix from %s", FEATURES_PATH)
        df = spark.read.parquet(FEATURES_PATH)
        
        # We need year and month for temporal splitting.
        # Train: Jul 2013 - Apr 2014
        # Test: May 2014 - Jun 2014
        train_df = df.filter((col("year") == 2013) | ((col("year") == 2014) & (col("month") <= 4)))
        test_df = df.filter((col("year") == 2014) & (col("month") >= 5))
        
        logger.info(f"Train set size: {train_df.count()} rows")
        logger.info(f"Test set size: {test_df.count()} rows")

        # ==================================================================
        # STEP 2: ML Pipeline Construction
        # ==================================================================
        feature_cols = [
            "hour_of_day", "day_of_week", "is_weekend", "is_friday",
            "zone_id", "zone_population_density",
            "zone_type_residential", "zone_type_commercial", "zone_type_transit_hub",
            "is_raining", "temperature_bucket",
            "demand_lag_1d", "demand_lag_7d", "rolling_7d_mean"
        ]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
        scaler = StandardScaler(inputCol="raw_features", outputCol="features")
        gbt = GBTRegressor(labelCol="trip_count", featuresCol="features", maxIter=50)
        
        pipeline = Pipeline(stages=[assembler, scaler, gbt])

        # ==================================================================
        # STEP 3: Hyperparameter Tuning
        # ==================================================================
        logger.info("Starting CrossValidator tuning (3 folds)...")
        grid = ParamGridBuilder().addGrid(gbt.maxDepth, [5, 7]).build()
        evaluator = RegressionEvaluator(labelCol="trip_count", metricName="rmse")
        
        cv = CrossValidator(
            estimator=pipeline, 
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=3
        )
        
        cv_model = cv.fit(train_df)
        best_pipeline = cv_model.bestModel
        
        # Find best maxDepth
        best_gbt = best_pipeline.stages[-1]
        logger.info(f"Best GBT parameters: maxDepth={best_gbt.getMaxDepth()}")

        # ==================================================================
        # STEP 4: Evaluation vs Baseline
        # ==================================================================
        logger.info("Evaluating GBT vs Naive Baseline on Test Set...")
        
        # Predictions
        predictions = best_pipeline.transform(test_df)
        
        # RMSE
        gbt_rmse = evaluator.evaluate(predictions)
        
        # MAE
        evaluator_mae = RegressionEvaluator(labelCol="trip_count", metricName="mae")
        gbt_mae = evaluator_mae.evaluate(predictions)
        
        # Baseline Naive Model (predict demand_lag_7d)
        baseline_test_df = test_df.withColumn("trip_count_double", col("trip_count").cast("double")) \
                                  .withColumn("demand_lag_7d_double", col("demand_lag_7d").cast("double"))
        
        baseline_evaluator = RegressionEvaluator(labelCol="trip_count_double", predictionCol="demand_lag_7d_double", metricName="rmse")
        baseline_rmse = baseline_evaluator.evaluate(baseline_test_df)
        
        logger.info("--- TEST SET EVALUATION ---")
        logger.info(f"Naive Baseline RMSE: {baseline_rmse:.4f}")
        logger.info(f"GBT Model RMSE:      {gbt_rmse:.4f}")
        logger.info(f"GBT Model MAE:       {gbt_mae:.4f}")
        
        if gbt_rmse < baseline_rmse:
            logger.info("✅ SUCCESS: GBT model outperforms baseline.")
        else:
            logger.warning("❌ WARNING: GBT model did NOT outperform baseline.")

        # Per-zone evaluation
        logger.info("Computing per-zone RMSE...")
        zones = predictions.select("zone_id").distinct().orderBy("zone_id").collect()
        
        with open(TABLE_OUTPUT, "w") as f:
            f.write("# ML Evaluation Table: Per-Zone RMSE Comparison\n\n")
            f.write("| Zone ID | Baseline RMSE | GBT RMSE | Improvement % |\n")
            f.write("|---------|---------------|----------|---------------|\n")
            
            for row in zones:
                z = row["zone_id"]
                z_df = predictions.filter(col("zone_id") == z)
                z_baseline_df = baseline_test_df.filter(col("zone_id") == z)
                z_base = baseline_evaluator.evaluate(z_baseline_df)
                z_gbt = evaluator.evaluate(z_df)
                imp = ((z_base - z_gbt) / z_base * 100) if z_base > 0 else 0
                f.write(f"| {z} | {z_base:.2f} | {z_gbt:.2f} | {imp:.1f}% |\n")
                
        logger.info(f"Per-zone evaluation saved to {TABLE_OUTPUT}")

        # ==================================================================
        # STEP 5: Feature Importances
        # ==================================================================
        importances = best_gbt.featureImportances.toArray()
        fi_df = pd.DataFrame({"Feature": feature_cols, "Importance": importances})
        fi_df = fi_df.sort_values("Importance", ascending=False)
        
        # Plot
        plt.figure(figsize=(10, 6))
        plt.barh(fi_df["Feature"], fi_df["Importance"], color="skyblue")
        plt.gca().invert_yaxis()
        plt.title("GBT Model Feature Importances")
        plt.xlabel("Importance Score")
        plt.tight_layout()
        plt.savefig(CHART_OUTPUT)
        plt.close()
        
        logger.info(f"Feature importances plot saved to {CHART_OUTPUT}")
        
        # Save values to MinIO
        fi_str = fi_df.to_string(index=False)
        spark.sparkContext.parallelize([fi_str]).coalesce(1).saveAsTextFile(IMPORTANCE_OUTPUT_PATH)

        # ==================================================================
        # STEP 6: Save Model
        # ==================================================================
        logger.info(f"Saving PipelineModel to {MODEL_OUTPUT_PATH}...")
        best_pipeline.write().overwrite().save(MODEL_OUTPUT_PATH)
        
        logger.info("Model Training and Validation fully complete!")

    finally:
        spark.stop()

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("Job complete in %.1f seconds", elapsed)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
