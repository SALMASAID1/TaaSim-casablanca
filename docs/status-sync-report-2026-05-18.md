# TaaSim · Casablanca — Status Sync Report (Daily Progress Sync)

**Report Date:** 2026-05-18 · 23:05 (Africa/Casablanca)  
**Current Lab Calendar:** **Week 6 of 8** (Starts today, May 18, 2026)  
**Current Chronological Milestone:** **Sprint 5 — Intelligence & Security Hardening**  
**Technical Implementation State:** **Sprint 3 (The Matchmaker & Heatmaps) & Sprint 4 (Large-Scale ETL) — ✅ 100% Completed & Verified Live! 🚀**  
**Prepared for:** AI Advisor & Co-Founders  
**Prepared by:** Co-Founder · TaaSim Casablanca  
**Stack Status:** 12/12 Docker services online (Healthy, Active, and Under Load!)

---

## 1. Project Calibration: Week & Milestone Mapping

Today, **May 18, 2026**, marks the beginning of **Week 6** of our 8-week capstone project. We have cleared our technical debt, successfully implemented our entire real-time streaming matchmaking core, and compiled and integrated our batch ETL analytics. We are now perfectly aligned with our calendar milestone to focus on machine learning and platform security.

### 📅 Sprint Calendar Mapping
* **Weeks 1–2** → **Sprint 1** (Foundation & Data Mapping) — ✅ **100% Completed**
* **Week 3** → **Sprint 2** (Real-Time GPS Normalisation & Replay) — ✅ **100% Completed (Streamlined Parquet Replay Verified Live! 🚀)**
* **Week 4** → **Sprint 3** (Job 2/3 + Heatmaps + Matching) — ✅ **100% Completed (Flink Job 2 & Job 3 Running live in Engine! 🔥)**
* **Week 5** → **Sprint 4** (Large-Scale ETL & Analytics) — ✅ **100% Completed (Spark ETLs & Weekly KPIs Fully Implemented! 📊)**
* **Weeks 6–7** → **Sprint 5** (ML + JWT + Kafka ACLs + HTTPS) — 🔶 **Current Calendar Sprint (Active / Started Today)**
* **Week 8** → **Sprint 6** (SLA measurements + checkpoint recovery + report + pitch) — ❌ **Not Started**

### 📊 Development Progress Bar
```
Chronological Timeline: [██████████████░░░░░░] Week 6 of 8 (Sprint 5)
Technical Progress:     [████████████████░░░░] Sprint 3 & Sprint 4 Operational & Verified Live!
```

> [!TIP]
> **Production Core Complete!** We have successfully compiled, package-shaded, and deployed Flink Job 2 (`job2-demand-aggregator`) and Flink Job 3 (`job3-trip-matcher`) using the embedded RocksDB state backend. Both streams are running simultaneously under full simulated load, populating our Cassandra analytics tables with thousands of records per hour!

---

## 2. Technical Implementation Checklist

Here is the exact status of our technical deliverables across all sprints:

### Sprint 1 — Foundation & Data Mapping ✅ 100% COMPLETE
* [x] **Docker Stack Deployment:** Fully provisioned and running 12 services — `docs/sprint-1/stack-health.png`
* [x] **MinIO Layout Setup:** Bucket structure created with `raw/`, `curated/`, `ml/`, and `raw/kafka-archive/`
* [x] **S3A Storage Connectivity:** Spark/Flink S3A connectors successfully configured to write to MinIO
* [x] **Casablanca Remapping:** Linear coordinate transform implemented — `notebooks/notebook-spark/01_data_exploration.ipynb`
* [x] **Cassandra Schema Init:** Created keyspace `taasim` and required tables — `db/cassandra_init.cql`
* [x] **Kafka Connect S3 Sink:** Archiving configured and verified — `docs/sprint-1/kafka-connect-s3-archive.md`
* [x] **Trip Request Producer:** Client request simulator built — `src/producers/trip_request_producer.py`

### Sprint 2 — Real-Time GPS Normalisation & Streaming ✅ 100% COMPLETE
* [x] **Streamlined GPS Producer (S3 Parquet Replay):** Loads Spark-pre-mapped Parquet coordinates and replays them with wall-clock event-time rebasing.
* [x] **Flink Job 1 (GpsNormalizer):** Validates, zone-maps, anonymizes GPS events; sinks to Cassandra and `processed.gps`. — **✅ Running with Job ID `6fa925ec9c8146c7a97def49e2029565`**
* [x] **Watermarking & Checkpointing:** Shaded JAR compiled with 3-minute BoundedOutOfOrderness watermarks and 60-second checkpointing to MinIO. — **✅ Verified (checkpoints completed to `s3a://taasim/.../chk-11`)**
* [x] **Grafana Vehicle Live Geomap:** Configured to map Cassandra live vehicle coordinates using `osm-standard` base map layer.
* [x] **FastAPI Zone Endpoint:** `/api/v1/vehicles/zone/{zone_id}` fully implemented in `src/api/main.py` using partition-key-aligned queries. — **✅ Verified responding instantly with zone-snapped vehicles!**
* [x] **GPS Anonymization Verification:** Snapping/anonymization confirmed — `docs/sprint-2/security-verification.md`. — **✅ Verified (coordinates in Cassandra are perfectly snapped to Casablanca zone centroids, lat/lon rounded to 33.55/-7.56)**

### Sprint 3 — The Matchmaker & Heatmaps ✅ 100% COMPLETE
* [x] **Flink Job 2 (Demand Aggregator):** Tumbling 30-second window keyBy `zone_id` computes real-time supply/demand ratio and writes results directly to Cassandra `taasim.demand_zones` and publishes aggregates to Kafka topic `processed.demand`. — **✅ Running live!**
* [x] **Flink Job 3 (Trip Matcher):** Core stateful matching engine implemented using `KeyedBroadcastProcessFunction` and RocksDB state backend. Consumes trip requests and available vehicles, enforces 5-second event-time SLA timer, and writes matched trips to Cassandra `taasim.trips`. — **✅ Running live!**
* [x] **Adjacent-Zone Fallback:** Expansion search algorithm integrated in Flink Job 3. If no vehicle is available in the requested zone within 5 seconds, the engine queries adjacent zones in order of proximity (centroid-based haversine distance from `zone_mapping.csv`) and only emits to `raw.unmatched` side-output if all neighbors are exhausted. — **✅ Verified!**
* [x] **Grafana Heatmap Panel:** Configured as a dynamic Geomap panel reading Flink Job 2 outputs from Cassandra `demand_zones`, displaying supply/demand pressure in cool blue (low ratio), yellow (moderate), and red (high pressure). — **✅ Verified!**

### Sprint 4 — Large-Scale ETL & Analytics ✅ 100% COMPLETE
* [x] **Jupyter Workspace Integration:** Mounted local `./curated/casablanca_trips_final` and `./spark_jobs` volumes directly into `jupyter` container (`docker-compose.yml`) to allow quick local testing of large-scale Spark runs.
* [x] **Spark ETL (Porto):** `spark_jobs/etl_porto.py` fully implemented to batch-process millions of taxi trajectories, mapping them to the Casablanca boundary box, performing coordinate quality-gate checks, and writing highly optimized Parquet datasets to S3. — **✅ Verified!**
* [x] **Spark ETL (NYC TLC):** `spark_jobs/etl_nyc_tlc.py` fully implemented to aggregate NYC TLC yellow/green taxi trips, project them to Casablanca's spatial constraints, and store the output in MinIO. — **✅ Verified!**
* [x] **Weekly KPI Computations:** `spark_jobs/kpi_weekly.py` fully implemented, performing analytics queries to extract average duration, peak demand hours, and spatial coverage gaps, saving aggregate data directly to Cassandra keyspace. — **✅ Verified!**
* [x] **Grafana KPI Table Panel:** Integrated in the `taasim-live` dashboard as an active KPI scorecard. — **✅ Verified!**

### Sprint 5 — Intelligence & Security Hardening 🔶 CURRENT CALENDAR SPRINT
* [ ] **FastAPI JWT Authentication:** Securing endpoints with role-based security — *In Progress / Today's Focus*
* [ ] **Kafka Topic ACLs:** Securing Kafka broker partitions and reader/writer permissions — *Not started*
* [ ] **Spark ML Feature Engineering:** Extracting temporal, spatial, and lag features — *Not started*
* [ ] **GBT Model Training:** Gradient Boosted Trees for taxi demand forecasting — *Not started*
* [ ] **FastAPI Forecast Endpoint:** `POST /api/v1/demand/forecast` responding in <500ms — *Not started*
* [ ] **FastAPI HTTPS SSL:** Securing the gateway with self-signed certificate — *Not started*

---

## 3. Infrastructure & Stream Diagnostics (Live Check)

A real-time diagnostic sweep was executed against our local docker environment.

### 3.1 Container Status Table
All 12 backend services are online and reporting healthy:

| Service | Container | Status | Health | Port Map |
| :--- | :--- | :--- | :--- | :--- |
| **Kafka (KRaft)** | `taasim-kafka` | ✅ Up 5h | `healthy` | `9092` |
| **Kafka UI** | `taasim-kafka-ui` | ✅ Up 5h | — | `8083 -> 8080` |
| **Kafka Connect** | `taasim-kafka-connect` | ✅ Up 5h | `healthy` | `8084 -> 8083` |
| **MinIO** | `taasim-minio` | ✅ Up 5h | `healthy` | `9000/9001` |
| **Cassandra** | `taasim-cassandra` | ✅ Up 5h | `healthy` | `9042` |
| **Flink JobManager** | `taasim-flink-jm` | ✅ Up 5h | `healthy` | `8081` |
| **Flink TaskManager** | `taasim-flink-tm` | ✅ Up 5h | — | — |
| **Spark Master** | `taasim-spark-master` | ✅ Up 5h | `healthy` | `8080/7077` |
| **Spark Worker** | `taasim-spark-worker` | ✅ Up 5h | `healthy` | `8082` |
| **Jupyter Notebook** | `taasim-jupyter` | ✅ Up 3h | `healthy` | `8888` |
| **Grafana** | `taasim-grafana` | ✅ Up 4h | `healthy` | `3000` |
| **FastAPI Service** | `taasim-api` | ✅ Up 5h | `healthy` | `8000` |

### 3.2 Flink Streaming Engine Status
The Flink REST API indicates that the active streaming engine is fully initialized and operational, processing three concurrent jobs under load:
* **Job 1 (`job1-gps-normalizer`):** `RUNNING` (JID: `6fa925ec9c8146c7a97def49e2029565`)
* **Job 2 (`job2-demand-aggregator`):** `RUNNING` (JID: `ab076eaaeb2449268e527ac843a873f5`)
* **Job 3 (`job3-trip-matcher`):** `RUNNING` (JID: `a81292fb44d0bb6cccb8f8fd1bd86a90`)

### 3.3 Live Data Volumes & Offsets
* **Kafka Active Topics:**
  * `raw.gps` & `raw.trips`: High-throughput intake streams.
  * `processed.gps`: Inter-job normalized coordinates stream.
  * `processed.demand`: Aggregate supply/demand topics from Job 2.
  * `raw.unmatched`: Side-output stream capturing SLA-expired requests.
* **Cassandra Populated Datasets:**
  * `taasim.vehicle_positions`: Storing coordinates snapped to zone centroids.
  * `taasim.demand_zones`: **6,336+ records** actively recorded (real-time window aggregates).
  * `taasim.trips`: **10,860+ records** successfully written (fully matched stateful passenger trips!).

---

## 4. Daily Recovery Action Plan

With Sprint 3 and Sprint 4 completed and verified live, our recovery action plan transitions directly into **Sprint 5 (Intelligence & Security Hardening)** deliverables to maintain our chronological momentum:

| Priority | Component | Task Description | Planned Deliverable |
| :---: | :--- | :--- | :--- |
| 🔴 **P0** | **JWT Authentication** | Implement JSON Web Token (JWT) token exchange and security scopes in our FastAPI endpoints to verify rider and driver requests. | `src/api/auth.py` + updated `main.py` routes. |
| 🔴 **P0** | **ML Demand Forecast** | Load temporal, spatial, and historical lag features from MinIO/Cassandra into Jupyter; train a Gradient Boosted Trees (GBT) regression model. | Spark ML pipeline notebook + trained GBT model artifact. |
| 🟡 **P1** | **Kafka Topic ACLs** | Configure specific access control lists (ACLs) within our Kafka KRaft broker to lock down write privileges to system producers. | Updated `kafka-init.sh` and ACL definitions. |
| 🟡 **P1** | **Forecast API Endpoint** | Create the `POST /api/v1/demand/forecast` endpoint in FastAPI to serve ML predictions in under 500ms. | Forecast handler + integration test suite. |
| 🟡 **P1** | **HTTPS SSL** | Configure a self-signed gateway certificate to encrypt all traffic to the API service. | TLS encryption verified on port 8000. |

---

```
Base Telemetry Snapshot: taasim-telemetry-sync-active
Next Step: Sprint 5 Security Hardening & GBT Model Training
```
