# TaaSim · Casablanca — Status Sync Report (Daily Progress Sync)

**Report Date:** 2026-05-18 · 17:55 (Africa/Casablanca)  
**Current Lab Calendar:** **Week 6 of 8** (Starts today, May 18, 2026)  
**Current Chronological Milestone:** **Sprint 5 — Intelligence & Security Hardening**  
**Technical Implementation State:** **Sprint 3 — The Matchmaker & Heatmaps (In Progress & Stream Running! 🚀)**  
**Prepared for:** AI Advisor & Co-Founders  
**Prepared by:** Co-Founder · TaaSim Casablanca  
**Stack Status:** 12/12 Docker services online (Healthy, Active, and Under Load!)

---

## 1. Project Calibration: Week & Milestone Mapping

Today, **May 18, 2026**, marks the beginning of **Week 6** of our 8-week capstone project. 

### 📅 Sprint Calendar Mapping
* **Weeks 1–2** → **Sprint 1** (Foundation & Data Mapping) — ✅ **100% Completed**
* **Week 3** → **Sprint 2** (Real-Time GPS Normalisation) — ✅ **100% Completed (Verified Live! 🚀)**
* **Week 4** → **Sprint 3** (Job 2/3 + Heatmaps + Matching) — 🔶 **In Progress (Active Pipeline / Flink Jobs 2 & 3 Pending)**
* **Week 5** → **Sprint 4** (Large-Scale ETL & Analytics) — ⚠️ **0% Completed (Critical Path Delay)**
* **Weeks 6–7** → **Sprint 5** (ML + JWT + Kafka ACLs + HTTPS) — 🔶 **Current Calendar Sprint (Not Started)**
* **Week 8** → **Sprint 6** (SLA measurements + checkpoint recovery + report + pitch) — ❌ **Not Started**

### 📊 Development Progress Bar
```
Chronological Timeline: [██████████████░░░░░░] Week 6 of 8 (Sprint 5)
Technical Progress:     [████████░░░░░░░░░░░░] Sprint 2 Fully Live & Operational
```

> [!TIP]
> **Stream Engine Activated!** We have successfully kicked off both producers (GPS and Trip Requests) and submitted **Flink Job 1 (GpsNormalizer)**. Real-time remapped Casablanca stream is now live and flowing end-to-end!

---

## 2. Technical Implementation Checklist

Here is the exact status of our technical deliverables across all sprints:

### Sprint 1 — Foundation & Data Mapping ✅ 100% COMPLETE
* [x] **Docker Stack Deployment:** Fully provisioned and running 12 services — `docs/sprint-1/stack-health.png`
* [x] **MinIO Layout Setup:** Bucket structure created with `raw/`, `curated/`, `ml/`, and `raw/kafka-archive/`
* [x] **S3A Storage Connectivity:** Spark/Flink S3A connectors successfully configured to write to MinIO
* [x] **Casablanca Remapping:** Linear coordinate transform implemented — `notebooks/notebook-spark/01_data_exploration.ipynb`
* [x] **Vehicle GPS Producer:** Replay engine with Porto bbox mapping built — `src/producers/vehicle_gps_producer.py`
* [x] **Cassandra Schema Init:** Created keyspace `taasim` and required tables — `db/cassandra_init.cql`
* [x] **Kafka Connect S3 Sink:** Archiving configured and verified — `docs/sprint-1/kafka-connect-s3-archive.md`
* [x] **Trip Request Producer:** Client request simulator built — `src/producers/trip_request_producer.py`

### Sprint 2 — Real-Time GPS Normalisation ✅ 100% COMPLETE (VERIFIED LIVE)
* [x] **Flink Job 1 (GpsNormalizer):** Validates, zone-maps, anonymizes GPS events; sinks to Cassandra and `processed.gps`. (8 Java files implemented under `flink_jobs/`) — **✅ Running with Job ID `0c84960f7bd7a460b3b4fdada9231c19`**
* [x] **Watermarking & Checkpointing:** Shaded JAR compiled with 3-minute BoundedOutOfOrderness watermarks and 60-second checkpointing to MinIO. — **✅ Verified (11+ checkpoints completed, 0 failed to `s3a://taasim/.../chk-11`)**
* [x] **Grafana Vehicle Live Geomap:** provisioned via `grafana/provisioning/` using the Cassandra plugin and Geomap panel.
* [x] **FastAPI Zone Endpoint:** `/api/v1/vehicles/zone/{zone_id}` fully implemented in `src/api/main.py` using partition-key-aligned queries. — **✅ Verified responding instantly with zone-snapped vehicles!**
* [x] **GPS Anonymization Verification:** Snapping/anonymization confirmed — `docs/sprint-2/security-verification.md`. — **✅ Verified (coordinates in Cassandra are perfectly snapped to Casablanca zone centroids, lat/lon rounded to 33.55/-7.56)**

### Sprint 3 — The Matchmaker & Heatmaps 🔶 IN PROGRESS (PRODUCERS LIVE)
* [ ] **Flink Job 2 (Demand Aggregator):** Tumbling 30s window computing supply/demand ratio per zone — *Not started*
* [ ] **Flink Job 3 (Trip Matcher):** Stateful matching with RocksDB state backend — *Not started*
* [ ] **Grafana Heatmap Panel:** Visualizing demand density dynamically in Grafana — *Not started*
* [ ] **Adjacent-Zone Fallback:** Expansion search algorithm for Flink Job 3 — *Not started*

### Sprint 4 — Large-Scale ETL & Analytics ⚠️ 0% COMPLETED (DELAYED)
* [ ] **Spark ETL (Porto):** Batch processing Porto trajectories to Parquet — *Not started*
* [ ] **Spark ETL (NYC TLC):** Aggregating NYC TLC dataset (10M+ rows/month) — *Not started*
* [ ] **Weekly KPI Computations:** Analytics queries for AVG duration, peaks, and coverage gaps — *Not started*
* [ ] **Grafana KPI Table Panel:** Dynamic business KPI scorecard dashboard — *Not started*

### Sprint 5 — Intelligence & Security Hardening 🔶 CURRENT CALENDAR SPRINT
* [ ] **FastAPI JWT Authentication:** Securing endpoints with role-based security — *Not started*
* [ ] **Kafka Topic ACLs:** Securing Kafka broker partitions and reader/writer permissions — *Not started*
* [ ] **Spark ML Feature Engineering:** Extracting temporal, spatial, and lag features — *Not started*
* [ ] **GBT Model Training:** Gradient Boosted Trees for taxi demand forecasting — *Not started*
* [ ] **FastAPI Forecast Endpoint:** `POST /api/v1/demand/forecast` responding in <500ms — *Not started*
* [ ] **FastAPI HTTPS SSL:** Securing the gateway with self-signed certificate — *Not started*

---

## 3. Infrastructure & Stream Diagnostics (Live Check)

A real-time diagnostic sweep was executed against our local docker environment at **15:45 UTC+01:00**.

### 3.1 Container Status Table
All 12 backend services are online and reporting healthy:

| Service | Container | Status | Health | Port Map |
| :--- | :--- | :--- | :--- | :--- |
| **Kafka (KRaft)** | `taasim-kafka` | ✅ Up 3h | `healthy` | `9092` |
| **Kafka UI** | `taasim-kafka-ui` | ✅ Up 3h | — | `8083 -> 8080` |
| **Kafka Connect** | `taasim-kafka-connect` | ✅ Up 3h | `healthy` | `8084 -> 8083` |
| **MinIO** | `taasim-minio` | ✅ Up 3h | `healthy` | `9000/9001` |
| **Cassandra** | `taasim-cassandra` | ✅ Up 5m | `healthy` | `9042` |
| **Flink JobManager** | `taasim-flink-jm` | ✅ Up 3h | `healthy` | `8081` |
| **Flink TaskManager** | `taasim-flink-tm` | ✅ Up 3h | — | — |
| **Spark Master** | `taasim-spark-master` | ✅ Up 5m | `healthy` | `8080/7077` |
| **Spark Worker** | `taasim-spark-worker` | ✅ Up 5m | `healthy` | `8082` |
| **Jupyter Notebook** | `taasim-jupyter` | ✅ Up 5m | `healthy` | `8888` |
| **Grafana** | `taasim-grafana` | ✅ Up 3h | `healthy` | `3000` |
| **FastAPI Service** | `taasim-api` | ✅ Up 4m | `healthy` | `8000` |

### 3.2 Kafka Connect Integrations
Both S3 archiving connectors are deployed, actively running, and archiving Kafka events to MinIO:
* `s3-sink-raw-gps` (Status: **RUNNING** ✅ · Lag: active)
* `s3-sink-raw-trips` (Status: **RUNNING** ✅ · Lag: active)

### 3.3 Stream Activity Diagnostics
* **Kafka Topics & Offsets:**
  * `raw.gps`: **12,564 raw events** consumed (flowing active)
  * `raw.trips`: **12,910 raw events** consumed (flowing active)
  * `processed.gps`: **6,885 normalized events** produced by Flink Job 1 (flowing active)
* **Cassandra Data Volumes:**
  * `taasim.vehicle_positions`: **1,023 rows** written (actively increasing!)
  * `taasim.trips`: **0 rows** (Job 3 not started)
  * `taasim.demand_zones`: **0 rows** (Job 2 not started)
* **Flink Jobs:**
  * **Job 1 (`job1-gps-normalizer`):** `RUNNING` (JID: `0c84960f7bd7a460b3b4fdada9231c19`)
  * **Checkpoints:** 143 completed, 0 failed. Latest checkpoint `#143` externalized to MinIO: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/0c84960f7bd7a460b3b4fdada9231c19/chk-143`

---

## 4. Technical Verification & Security Audit

### 4.1 FastAPI Verification
A live query was sent to `GET /api/v1/vehicles/zone/15` and returned data immediately:
```json
[
  {
    "taxi_id": "20000007",
    "lat": 33.55,
    "lon": -7.5625,
    "status": "available",
    "event_time": "2026-05-18T16:54:08Z"
  },
  {
    "taxi_id": "20000007",
    "lat": 33.55,
    "lon": -7.5625,
    "status": "available",
    "event_time": "2026-05-18T16:54:06Z"
  }
]
```

### 4.2 Anonymization Audit
A verification of raw write logs in `taasim.vehicle_positions` confirms the security constraint is successfully met.
* Latitude and longitude values written are strictly snapped to Casablanca zone centroids (e.g. `33.55`, `-7.5625`), rather than displaying the high-precision floating coordinates replayed from Porto data. Raw geographic coordinates do not bypass the normalizer.

---

## 5. Daily Recovery Action Plan

With Sprint 2 verified, we will now turn our focus to implementing the remaining Flink and Spark components:

| Priority | Component | Task Description | Planned Deliverable |
| :---: | :--- | :--- | :--- |
| 🔴 **P0** | **Sprint 3 (Flink)** | Design and implement **Flink Job 2 (Demand Aggregator)** using a 30s tumbling window to write supply/demand aggregates. | Flink Job 2 code + shaded build + deployment to Flink JM. |
| 🔴 **P0** | **Sprint 3 (Flink)** | Design and implement **Flink Job 3 (Trip Matcher)** with RocksDB state backend to compute matches and ETAs. | Flink Job 3 code + state validation tests. |
| 🟡 **P1** | **Grafana Panel** | Configure Grafana's demand heatmap panel using Flink Job 2 Cassandra outputs. | Dynamic visual supply-demand map. |
| 🟡 **P1** | **Sprint 4 (Spark)** | Implement the Spark ETL notebooks for Porto CSV and NYC TLC datasets. | Parquet outputs stored in MinIO storage (`curated/`). |

---

```
┌─────────────────────────────────────────────────────────────────────┐
│  TaaSim · Casablanca — Daily Status Snapshot (2026-05-18)          │
│                                                                     │
│  Timeline     ██████████████░░░░░░  Week 6 of 8 · Sprint 5          │
│  Progress     ████████░░░░░░░░░░░░  Sprint 2 Fully Operational 🚀   │
│  Docker Stack ████████████████████  12/12 Services Green            │
│  Flink Jobs   ███████░░░░░░░░░░░░░  1/3 Running (Job 1 Active)      │
│  Data Flow    ████████████████████  Producers Active · Offsets ↑     │
│                                                                     │
│  NEXT ACTION → Develop Flink Job 2 (Demand Aggregator)              │
└─────────────────────────────────────────────────────────────────────┘
```

---
*Report compiled automatically from live environment telemetry against local Docker daemon, Kafka brokers, Cassandra nodes, Flink REST API, and local git state.*
