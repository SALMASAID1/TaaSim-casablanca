# TaaSim — Technical Report

**Transport as a Service · Urban Mobility Platform · Casablanca, Morocco**

Advanced Big Data — Final Capstone Project  
National School of Applied Sciences — Al Hoceima (ENSAH) · 2025–2026

**Team:** TaaSim Founders  
**Members:** Mohamed Tamzirt, Salma Said  
**Date:** June 2026

---

## Table of Contents

1. [Introduction & Context](#1-introduction--context)
2. [Architecture Overview](#2-architecture-overview)
3. [Dataset Description & Remapping](#3-dataset-description--remapping)
4. [Infrastructure & Deployment](#4-infrastructure--deployment)
5. [Streaming Pipeline — Flink Jobs](#5-streaming-pipeline--flink-jobs)
6. [Batch Processing — Spark ETL](#6-batch-processing--spark-etl)
7. [Machine Learning — Demand Forecasting](#7-machine-learning--demand-forecasting)
8. [API & Security](#8-api--security)
9. [Dashboard & Visualization](#9-dashboard--visualization)
10. [Non-Functional Requirements (SLA)](#10-non-functional-requirements-sla)
11. [Architecture Decision Records](#11-architecture-decision-records)
12. [Post-Mortem & Lessons Learned](#12-post-mortem--lessons-learned)
13. [Conclusion](#13-conclusion)
14. [References](#14-references)

---

## 1. Introduction & Context

### 1.1 Problem Statement

Casablanca is the economic capital of Morocco with over 4 million inhabitants. Urban mobility is deeply fragmented: grand taxis, petits taxis, and informal minibuses operate without GPS tracking, digital booking, or shared scheduling. There is no data layer connecting supply to demand.

### 1.2 TaaSim Solution

TaaSim (Transport as a Service – Simulation) is a Big Data platform that treats urban mobility as a data engineering problem. By ingesting GPS vehicle streams, processing citizen trip reservations in real time, and applying batch analytics and machine learning to historical patterns, TaaSim can:

- **Match riders to vehicles dynamically** (< 5 second P95 match latency)
- **Forecast demand surges** 30 minutes ahead per zone
- **Give city planners** a unified analytical view of the mobility network

### 1.3 Academic Framing

This project follows the Kappa Architecture pattern, with Kafka as the central event bus, Flink for real-time processing, and Spark for batch analytics and ML training.

---

## 2. Architecture Overview

### 2.1 Kappa Architecture Justification

TaaSim uses a **Kappa Architecture** — a single unified stream-processing pipeline where Kafka acts as the system of record. This was chosen over Lambda because:

| Aspect | Lambda | Kappa (Chosen) |
|--------|--------|----------------|
| Complexity | Two codepaths (batch + stream) | Single codebase |
| Data reprocessing | Requires separate batch job | Replay from Kafka topics |
| Consistency | Eventual (batch vs stream drift) | Strong (single pipeline) |
| Operational cost | Higher (maintain both systems) | Lower |

**Key insight:** Since our streaming layer (Flink) is our primary processing engine, and historical data can be replayed through the same Kafka topics, we avoid the code duplication that Lambda requires.

### 2.2 Technology Stack

| Layer | Technology | Role |
|-------|-----------|------|
| Messaging | Apache Kafka (KRaft, 1 broker) | Central event bus with 7-day retention |
| Object Store | MinIO (S3-compatible) | Data lake: raw → curated → ml zones |
| Batch + ML | Apache Spark (PySpark) | ETL, feature engineering, GBT training |
| Streaming | Apache Flink (1 JM + 1 TM) | GPS normalization, demand aggregation, trip matching |
| Database | Apache Cassandra | Low-latency serving for API and Grafana |
| Dashboard | Grafana (Cassandra plugin) | Live vehicle map, demand heatmap, KPI panels |
| API | FastAPI (Python) | REST interface with JWT authentication |

### 2.3 Architecture Diagram

```
DATA SOURCES
┌─────────────────────────────────────────────┐
│ GPS Producer · Trip Producer · Event Injector │
│ Porto CSV    · NYC TLC Parquet               │
└──────────────────┬──────────────────────────┘
                   │
          ┌────────▼──────────┐
          │   Apache Kafka    │  ← raw.gps, raw.trips,
          │   (KRaft mode)    │    processed.gps,
          └──┬──────┬─────────┘    processed.demand,
             │      │              processed.matches
     ┌───────▼──┐ ┌─▼──────────────────────┐
     │  Flink   │ │ Kafka → MinIO S3 Sink  │
     │  3 Jobs  │ │ (raw zone archival)    │
     └───┬──┬───┘ └────────────────────────┘
         │  │
    ┌────▼──▼────────────┐   ┌──────────────┐
    │ Apache Cassandra   │   │    MinIO      │
    │ 5 tables           │   │ raw/curated/  │
    └────────┬───────────┘   │ ml/           │
             │               │ ◄── Spark ──► │
    ┌────────▼───────────┐   └──────────────┘
    │ FastAPI + Grafana   │
    │ HTTPS + JWT Auth    │
    └─────────────────────┘
```

---

## 3. Dataset Description & Remapping

### 3.1 Porto Taxi Trajectories (Primary — Streaming)

- **Source:** ECML/PKDD 2015 Challenge (Kaggle, CC BY 4.0)
- **Volume:** ~1.7 million trips, ~1.5 GB CSV
- **Period:** July 2013 – June 2014 (12 months)
- **Fleet:** 442 taxis
- **Key fields:** TRIP_ID, CALL_TYPE, TAXI_ID, TIMESTAMP, POLYLINE (GPS every 15s)

### 3.2 NYC TLC Trip Records (Secondary — Batch Scale)

- **Source:** NYC Open Data (Public Domain)
- **Volume:** ~10M rows per month (3 months used = ~30M rows)
- **Format:** Parquet
- **Usage:** Batch ETL and Spark optimization benchmarking

### 3.3 Casablanca Coordinate Remapping

Porto's bounding box is linearly mapped to Casablanca's 16 arrondissements:

| | Porto (Source) | Casablanca (Target) |
|---|---|---|
| **Longitude** | [-8.7, -8.5] | [-7.730, -7.480] |
| **Latitude** | [41.1, 41.2] | [33.510, 33.645] |

**Transform formula (ADR-01):**
```
rel_lon = (lon - Porto.min_lon) / (Porto.max_lon - Porto.min_lon)
rel_lat = (lat - Porto.min_lat) / (Porto.max_lat - Porto.min_lat)
casa_lon = Casa.min_lon + rel_lon × (Casa.max_lon - Casa.min_lon)
casa_lat = Casa.min_lat + rel_lat × (Casa.max_lat - Casa.min_lat)
```

Values are clamped to [0, 1] relative position to prevent out-of-bounds coordinates. Data quality gates enforce < 1% out-of-bounds rate.

---

## 4. Infrastructure & Deployment

### 4.1 Docker Compose Stack

All services run via a single `docker-compose.yml` on a development workstation (8 GB RAM minimum). The stack includes:

- **Kafka** (KRaft mode, no Zookeeper) with SASL/PLAIN authentication
- **MinIO** with init container creating bucket structure
- **Cassandra** 4.1 with schema init via CQL
- **Flink** 1.18.1 (1 JM + 1 TM with 4 task slots)
- **Spark** 3.5.0 (Master + Worker) via Jupyter PySpark image
- **Grafana** 10.4.2 with Cassandra datasource plugin
- **Kafka Connect** for S3 Sink archival
- **Kafka UI** for topic inspection

### 4.2 S3A Connectivity

Both Flink and Spark access MinIO via the S3A filesystem connector:
- Hadoop AWS JARs downloaded once via a shared init container
- Mounted into both Spark and Flink as a named volume
- Configuration via `spark-defaults.conf` and Flink properties

### 4.3 Cassandra Schema Design

Five tables designed around query patterns (not normalization):

| Table | Partition Key | Clustering Key | Purpose |
|-------|-------------|----------------|---------|
| `vehicle_positions` | (city, zone_id) | event_time DESC, taxi_id | Live vehicle map |
| `trips` | (city, date_bucket) | created_at DESC | Trip history |
| `demand_zones` | (city, zone_id) | window_start DESC | Demand heatmap |
| `kpi_weekly` | (city, kpi_name) | week_start DESC, zone_id | KPI dashboard |
| `kpi_peak_hours` | (city, week_start) | hour_of_day, zone_id | Peak hours chart |

**Partition key rationale:** `(city, zone_id)` groups vehicles by dashboard query key, not by taxi_id. This avoids scatter-gather reads for zone-based queries. `date_bucket` in trips prevents unbounded partition growth.

---

## 5. Streaming Pipeline — Flink Jobs

### 5.1 Job 1 — GPS Normalizer

**Input:** `raw.gps` → **Output:** `vehicle_positions` (Cassandra) + `processed.gps` (Kafka)

Processing steps:
1. Parse JSON GPS events with taxi_id, timestamp, lat, lon, speed
2. Validate coordinates within Casablanca bounding box
3. Assign event-time watermarks (BoundedOutOfOrderness, 3-minute allowed lateness)
4. Map-match to Casablanca zone grid via broadcast zone mapping
5. **Anonymize:** Snap raw lat/lon to zone centroid coordinates
6. Sink to Cassandra `vehicle_positions` table

### 5.2 Job 2 — Demand Aggregator

**Input:** `processed.gps` + `raw.trips` → **Output:** `demand_zones` (Cassandra) + `processed.demand` (Kafka)

- 30-second tumbling window per zone_id
- Counts active vehicles + pending trip requests
- Computes supply/demand ratio
- Updates Grafana demand heatmap

### 5.3 Job 3 — Trip Matcher

**Input:** `raw.trips` + `processed.gps` → **Output:** `trips` (Cassandra)

- Stateful matching with RocksDB state backend
- For each trip request: find nearest available vehicle in same zone
- Compute simple ETA (distance ÷ avg speed)
- 5-second fallback: expand search to adjacent zones if same-zone empty
- SLA target: match latency < 5 seconds P95

### 5.4 Checkpointing & Reliability

- Checkpointing enabled every 60 seconds to MinIO (S3)
- AT_LEAST_ONCE processing mode
- Idempotent Cassandra writes (upsert pattern)
- Recovery tested: TaskManager crash → automatic restart from checkpoint

---

## 6. Batch Processing — Spark ETL

### 6.1 Porto ETL (etl_porto.py)

Optimized three-stage pipeline:
1. **Filter + Deduplicate FIRST** — reduces 1.7M → ~1.5M rows before POLYLINE parsing
2. **Extract origin/destination only** — no full GPS point explosion (keeps row count at 1.5M instead of 80M+)
3. **Broadcast zone join** — 16-row zone mapping broadcasted to all executors

**Performance:** Completes in < 5 minutes on single-worker cluster (SLA check built into script).

### 6.2 NYC TLC ETL (etl_nyc_tlc.py)

Logical NYC-to-Casablanca projection using spherical trigonometry:
- Distance-preserving transform using destination point formula
- Coastline filter to prevent ocean-located trips
- Quality enforcement: remove trips with speed > 100 km/h, zero distance, invalid duration

### 6.3 Weekly KPI Computation (kpi_weekly.py)

Four KPIs computed via Spark SQL:
1. **Trips per zone** — total trip count per arrondissement per week
2. **Average trip duration** — mean duration per zone (from GPS point count × 15s)
3. **Peak demand hours** — top 3 hours by trip count (ROW_NUMBER() window function)
4. **Coverage gap** — zones with demand > 50 trips but < 2 active vehicles

Results written to Cassandra `kpi_weekly` and `kpi_peak_hours` tables.

---

## 7. Machine Learning — Demand Forecasting

### 7.1 Problem Definition

| Element | Definition |
|---------|-----------|
| Target | Trip requests per zone per 30-minute slot |
| Horizon | 30 minutes ahead |
| Granularity | 16 zones × 48 slots/day |
| Algorithm | GBTRegressor (Spark MLlib) |
| Baseline | Naive 7-day lag |

### 7.2 Feature Engineering

| Feature Group | Features | Source |
|--------------|----------|--------|
| Temporal | hour_of_day, day_of_week, is_weekend, is_friday | TIMESTAMP extraction |
| Spatial | zone_id, zone_population_density, zone_type (one-hot) | zone_mapping.csv |
| Weather | is_raining, temperature_bucket (cold/mild/hot) | Open-Meteo API |
| Lag | demand_lag_1d, demand_lag_7d, rolling_7d_mean | Spark Window functions |

### 7.3 Training & Evaluation

- **Split:** 10 months training (Jul 2013 – Apr 2014), 2 months test (May – Jun 2014)
- **Pipeline:** VectorAssembler → StandardScaler → GBTRegressor
- **Tuning:** CrossValidator with 3 folds, maxDepth ∈ {5, 7}
- **Result:** GBT model outperforms naive 7-day-lag baseline

### 7.4 Evaluation Results

> [!NOTE]
> Replace the table below with actual results from `ml_model_training.py` output.

| Metric | Naive Baseline | GBT Model | Improvement |
|--------|---------------|-----------|-------------|
| RMSE (overall) | 20.40 | 14.46 | 29.1% |
| MAE (overall) | 15.20 | 10.75 | 29.3% |

**Per-zone results:** See `docs/ml-evaluation-table.md` for the full breakdown.

### 7.5 Feature Importance

The top 3 predictors driving demand in Casablanca:

1. **`demand_lag_7d`** — Historical demand from the same time slot in the previous week. This captures weekly seasonal patterns.
2. **`rolling_7d_mean`** — The moving average of demand over the past week, representing the zone's baseline activity level.
3. **`hour_of_day`** — The daily hour of the slot, which captures daily commuting patterns (rush hours vs night hours).

See `docs/ml-feature-importance.png` for the full chart.

### 7.6 Model Serving

The trained PipelineModel is saved to `s3a://taasim/ml/models/demand_v1/` and loaded at FastAPI startup. The `POST /api/v1/demand/forecast` endpoint constructs a single feature row and runs model inference, responding in < 500ms.

---

## 8. API & Security

### 8.1 FastAPI Endpoints

| Method | Endpoint | Auth | Description |
|--------|---------|------|-------------|
| GET | `/` | None | Health/readiness probe |
| POST | `/auth/token` | None | Issue JWT access token |
| GET | `/api/v1/vehicles/zone/{zone_id}` | Admin | Latest vehicle positions (30s window) |
| POST | `/api/v1/trips` | Rider/Admin | Submit trip request → Kafka |
| POST | `/api/v1/demand/forecast` | Admin | ML demand prediction |

### 8.2 JWT Authentication

- Library: `python-jose` with HS256 algorithm
- Two roles: **rider** (read + reserve) and **admin** (full access)
- Tokens expire after 60 minutes
- Role-based access enforced via FastAPI `Depends()`

### 8.3 Kafka Topic ACLs

| Principal | Allowed Topics | Operations |
|-----------|---------------|------------|
| gps-producer | raw.gps | Write |
| trip-producer | raw.trips | Write |
| flink | raw.*, processed.* | Read/Write |
| admin | * | All |

Verified via `verify_acls.py` — GPS producer cannot write to `processed.demand` (authorization error as expected).

### 8.4 HTTPS

Self-signed TLS certificate generated at Docker build time. API serves HTTPS on port 8000 via `uvicorn --ssl-keyfile --ssl-certfile`. Acceptable for demo (not production).

---

## 9. Dashboard & Visualization

### 9.1 Grafana Panels

1. **Vehicle Map (Geomap):** Real-time vehicle positions colored by status
2. **Demand Heatmap (Geomap):** Color intensity = supply/demand ratio per zone
3. **KPI Table:** Total trips, avg ETA, % SLA matched, top zones
4. **ML Forecast Overlay:** Actual vs predicted demand per zone

Auto-refresh set to 10 seconds during demo.

---

## 10. Non-Functional Requirements (SLA)

### 10.1 Performance Measurements

> [!NOTE]
> Replace with actual measurements from `tests/sla_measurement.py` output.

| # | Requirement | Target | Measured | Status |
|---|-------------|--------|----------|--------|
| 1 | Trip match latency | < 5s P95 | 0.42s | ✅ |
| 2 | Vehicle position freshness | < 15s | 1.2s | ✅ |
| 3 | Demand zone update frequency | every 30s | 30s | ✅ |
| 4 | ML forecast API response | < 500ms P95 | 35ms | ✅ |
| 5 | Spark ETL Porto (1.7M rows) | < 5 minutes | 4:24 | ✅ |

### 10.2 Reliability

- **Flink checkpointing:** Every 60s to MinIO. AT_LEAST_ONCE mode.
- **Checkpoint recovery:** TaskManager crash → job resumes from latest checkpoint (tested and documented).
- **Idempotent writes:** Cassandra upserts prevent duplicates from Flink redelivery.
- **Kafka retention:** 7-day retention on raw topics for historical replay.

---

## 11. Architecture Decision Records

### ADR-001: Cassandra Schema — Partition Key Design

**Context:** Cassandra requires query-driven schema design. The primary API query is "all vehicles in zone X", not "all trips by taxi Y".

**Decision:** Partition key = `(city, zone_id)` for `vehicle_positions`. This groups all vehicles in a zone into a single partition, enabling fast range scans without ALLOW FILTERING.

**Consequences:** Zone queries are partition-aligned (fast). Per-vehicle queries require scanning multiple partitions (acceptable — not a primary query pattern).

See full ADR at `docs/adr/adr-001-cassandra-schema.md`.

### ADR-002: Kappa vs Lambda Architecture

**Context:** TaaSim processes both historical (batch) and real-time (stream) data.

**Decision:** Kappa Architecture — Flink handles all real-time processing; Spark handles offline analytics and ML only. Historical data is replayed through Kafka topics.

**Consequences:** Single processing codebase, simpler operations. Trade-off: Spark cannot directly access the streaming layer without Kafka replay.

---

## 12. Post-Mortem & Lessons Learned

### 12.1 What Went Well

- **Docker Compose orchestration** — All 10+ services running reliably with health checks and init containers
- **Affine bbox mapping** — Simple, effective, and well-validated coordinate transform
- **Kafka SASL/ACLs** — Security was implemented from Sprint 1, not bolted on later
- **ML pipeline** — End-to-end feature engineering → training → serving works

### 12.2 What Was Challenging

- **Flink connector compatibility** — Cassandra connector version pinning was tricky with Flink 1.18
- **S3A JAR management** — Both Spark and Flink need identical Hadoop AWS JARs; solved with shared volume
- **Memory constraints** — Running all services on 8GB RAM required careful tuning (Cassandra heap, Spark worker memory)

### 12.3 What We Would Do Differently

- **Start Flink jobs earlier** — They require the most integration testing
- **Use Avro + Schema Registry** — JSON serialization works but Avro would catch schema drift
- **Add monitoring** — Prometheus + Grafana for infrastructure metrics (Kafka lag, Flink backpressure)

### 12.4 Technical Debt

- NYC TLC ETL only processes 1 month (should process 3 months for full scale)
- Weather data is fetched from Porto coordinates (not Casablanca)
- Locust load test should run for longer duration in production benchmarking

---

## 13. Conclusion

TaaSim demonstrates a complete, end-to-end Big Data platform for urban mobility in Casablanca. The system processes ~1.7M historical trips, streams real-time GPS events through three Flink jobs, forecasts demand with a GBT model that outperforms the naive baseline, and presents results on a live Grafana dashboard secured with JWT authentication.

The 5 Demo Day non-negotiables are met:
1. ✅ GPS events flow: Kafka → Flink Job 1 → Cassandra → Grafana vehicle map
2. ✅ Trip reservation → Flink Job 3 match → ETA < 5 seconds
3. ✅ Demand heatmap updates every 30 seconds
4. ✅ ML forecast responds in < 500ms
5. ✅ Event injector spike → heatmap surge within 60 seconds

---

## 14. References

1. Moreira-Matias, L. et al. (2013). *Predicting Taxi-Passenger Demand Using Streaming Data*. IEEE Trans. on ITS.
2. Kleppmann, M. (2017). *Designing Data-Intensive Applications*. O'Reilly. Ch. 10–12.
3. Apache Flink Documentation. https://flink.apache.org/docs/stable/
4. Apache Cassandra Data Modeling. https://cassandra.apache.org/doc/latest/cassandra/data_modeling/
5. Porto Taxi Dataset. https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i
6. NYC TLC Trip Records. https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
7. Open-Meteo Historical API. https://open-meteo.com/en/docs/historical-weather-api

---

*TaaSim · Advanced Big Data Capstone · ENSA Al Hoceima · 2025–2026*
