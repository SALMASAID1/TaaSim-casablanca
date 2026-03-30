# task05 — FastAPI Demand Forecast Endpoint

## Context
The `POST /api/v1/demand/forecast` endpoint is TaaSim's ML-serving interface. It loads the
trained GBT `PipelineModel` from MinIO at API startup, caches it in memory, and responds to
forecast requests in under 500 ms at 20 requests per second. This endpoint is what the Grafana
ML forecast overlay calls every 30 seconds per zone, and what the Week 8 load test verifies. It
is also the endpoint that transforms TaaSim from a reactive dispatcher into a proactive platform.

## Objective
Implement `POST /api/v1/demand/forecast` in FastAPI that loads the trained Spark `PipelineModel`
from MinIO at startup, applies it to a single (zone_id, datetime) input, and returns
`{predicted_demand, zone_id}` in under 500 ms at 20 req/s.

## Acceptance Criteria
- [ ] Endpoint `POST /api/v1/demand/forecast` implemented and accessible (admin role required)
- [ ] Request body: `{zone_id: int, datetime: str (ISO-8601)}`
- [ ] Response body: `{predicted_demand: float, zone_id: int, datetime: str}`
- [ ] `PipelineModel` loaded from `s3a://taasim/ml/models/demand_v1/` **once at API startup**
  and cached in `app.state.forecast_model` (never reloaded per request)
- [ ] Feature construction for inference matches training features exactly (same column names
  and types)
- [ ] Latency target: P95 response time < 500 ms at 20 req/s (verified with Locust)
- [ ] Locust load test script `tests/locustfile.py` provided in starter kit — run it and
  capture the results table as `docs/locust-forecast-results.png`
- [ ] **Panel 4 — ML Forecast Overlay**: Grafana dashboard updated to show bar chart comparing
  `pending_requests` (actual) vs `forecast_demand` (ML) per zone

## Technical Hints
- Load model at startup using FastAPI `lifespan`:
  ```python
  from pyspark.ml import PipelineModel

  @asynccontextmanager
  async def lifespan(app: FastAPI):
      spark = SparkSession.builder.appName("taasim-api").getOrCreate()
      app.state.forecast_model = PipelineModel.load("s3a://taasim/ml/models/demand_v1/")
      app.state.spark = spark
      yield
      spark.stop()
  ```
- Single-row prediction: construct a Spark DataFrame with one row from the request, apply the
  pipeline, extract the prediction:
  ```python
  feature_row = spark.createDataFrame([{
      "zone_id": zone_id, "hour_of_day": dt.hour, "day_of_week": dt.weekday(),
      "is_weekend": int(dt.weekday() >= 5), "is_friday": int(dt.weekday() == 4),
      # ... other features with sensible defaults for inference
  }])
  prediction = app.state.forecast_model.transform(feature_row)
  predicted_demand = prediction.select("prediction").collect()[0][0]
  ```
- For <500 ms latency: single-row Spark inference is acceptable on a local Spark session.
  If latency is still too high, consider exporting the model to ONNX or using pandas UDF.
- Locust load test example (from starter kit):
  ```python
  from locust import HttpUser, task
  class ForecastUser(HttpUser):
      @task
      def forecast(self):
          self.client.post("/api/v1/demand/forecast",
              json={"zone_id": 5, "datetime": "2024-01-15T08:30:00"},
              headers={"Authorization": f"Bearer {ADMIN_TOKEN}"})
  ```
- Reference: project brief §5.3 Training & Serving Steps (Step 6), §6.1 Performance (ML forecast row),
  §9.5 FastAPI Service (POST /api/v1/demand/forecast).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
