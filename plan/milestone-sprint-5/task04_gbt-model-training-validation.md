# task04 — GBT Model Training & Validation

## Context
This is TaaSim's ML centrepiece. A Gradient Boosted Trees regressor trained on the full Porto
feature matrix must outperform the naive 7-day-lag baseline on RMSE — this is a hard pass/fail
requirement from the project brief. Passing this task proves the platform adds predictive value
beyond simple historical lookup. The trained `PipelineModel` is saved to MinIO and loaded by the
FastAPI service at startup, making ML predictions available via REST in under 500 ms.

## Objective
Train a PySpark MLlib `GBTRegressor` on the Porto feature matrix with a temporal train/test
split, evaluate against the naive baseline, produce a feature importance chart, and save the
`PipelineModel` artifact to `s3a://taasim/ml/models/demand_v1/`.

## Acceptance Criteria
- [ ] Feature matrix loaded from `s3a://taasim/ml/features/`
- [ ] **Temporal train/test split**: first 10 months (Jul 2013 – Apr 2014) = train,
  last 2 months (May–Jun 2014) = test. Split performed with `filter()` on `year_month` column —
  **no random shuffle across time** (would leak future data into training)
- [ ] Spark ML `Pipeline` built: `VectorAssembler` → `StandardScaler` → `GBTRegressor`
- [ ] `CrossValidator` with `ParamGridBuilder`: test 2 values of `maxDepth` (5 and 7) only,
  3 folds (time constraint — not full grid search)
- [ ] **Evaluation**: RMSE and MAE computed on test set using `RegressionEvaluator`
- [ ] **Baseline**: naive model that predicts `demand_lag_7d` as the forecast, evaluated with
  the same RMSE metric on the same test set
- [ ] GBT model achieves **lower RMSE than the naive baseline** (mandatory pass criterion)
- [ ] Per-zone RMSE comparison table (GBT vs baseline) printed and saved to
  `docs/ml-evaluation-table.md`
- [ ] Feature importance chart saved to `docs/ml-feature-importance.png` with top 3 predictors
  identified and explained in business terms
- [ ] Trained model saved: `model.write().overwrite().save("s3a://taasim/ml/models/demand_v1/")`
- [ ] Feature importance values logged to `s3a://taasim/ml/models/demand_v1/feature_importances.txt`

## Technical Hints
- Pipeline construction:
  ```python
  from pyspark.ml import Pipeline
  from pyspark.ml.feature import VectorAssembler, StandardScaler
  from pyspark.ml.regression import GBTRegressor
  from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
  from pyspark.ml.evaluation import RegressionEvaluator

  feature_cols = ["hour_of_day","day_of_week","is_weekend","is_friday",
                  "zone_id","zone_population_density",
                  "zone_type_residential","zone_type_commercial","zone_type_transit_hub",
                  "is_raining","temperature_bucket",
                  "demand_lag_1d","demand_lag_7d","rolling_7d_mean"]

  assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
  scaler    = StandardScaler(inputCol="raw_features", outputCol="features")
  gbt       = GBTRegressor(labelCol="trip_count", featuresCol="features",
                            maxDepth=5, maxIter=50)
  pipeline  = Pipeline(stages=[assembler, scaler, gbt])
  ```
- CrossValidator (minimal grid):
  ```python
  grid = ParamGridBuilder().addGrid(gbt.maxDepth, [5, 7]).build()
  cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid,
                      evaluator=RegressionEvaluator(labelCol="trip_count",
                                                    metricName="rmse"),
                      numFolds=3)
  ```
- Feature importance: `cv_model.bestModel.stages[-1].featureImportances`
  — index back to `feature_cols` list to name them.
- Naive baseline RMSE:
  ```python
  from pyspark.sql.functions import col
  evaluator = RegressionEvaluator(labelCol="trip_count", predictionCol="demand_lag_7d",
                                   metricName="rmse")
  baseline_rmse = evaluator.evaluate(test_df)
  ```
- Reference: project brief §5 Spark ML Pipeline, §8.1 Minimum Viable Pipeline (point 4).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
