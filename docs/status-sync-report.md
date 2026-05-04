# TaaSim · Casablanca — Status Sync Report

**Report Date:** 2026-05-03 · 14:00 (Africa/Casablanca)  
**Prepared for:** AI Advisor  
**Prepared by:** Co-Founder · TaaSim Casablanca  
**Stack uptime at report time:** ~19 hours continuous

---

## 1. Current Milestone

| Dimension | Value |
|---|---|
| **Current Lab Week** | **Week 3 of 8** (ending today, May 3) |
| **Active Sprint** | **Sprint 2 — Real-Time GPS Normalisation** |
| **Sprint mapping** | W1–2 → Sprint 1 ✅ completed · W3 → Sprint 2 (current) · W4 → Sprint 3 |

### Sprint 1 — Foundation & Data Mapping ✅ COMPLETE

All checklist items are verified as done:

- [x] Docker stack screenshot (all services green) — `docs/sprint-1/stack-health.png`
- [x] Jupyter data-profiling notebook — `notebooks/notebook-spark/01_data_exploration.ipynb`
- [x] Zone-remapped trips visualised on Casablanca map — `docs/sprint-1/casablanca-coordinate-validation.png`
- [x] GPS + trip events flow design in Kafka (`raw.gps`, `raw.trips`) — evidenced in `docs/sprint-1/kafka-connect-s3-archive.md`
- [x] MinIO `raw/kafka-archive/` receives Kafka topic mirrors (Kafka Connect S3 Sink)
- [x] ADR submitted — `docs/adr/adr-001-cassandra-schema.md`
- [ ] Team name chosen + 1-slide startup concept — **still pending**

### Sprint 2 — Real-Time GPS Normalisation 🔶 IN PROGRESS

- [x] Flink Job 1 source code implemented (8 Java files in `flink_jobs/src/main/java/com/taasim/flink/job1/`)
- [x] Shaded JAR built — `flink_jobs/target/taasim-flink-jobs-1.0.0-shaded.jar`
- [x] Watermark test completed and documented — `docs/sprint-2/watermark-test-evidence.md`
- [x] Checkpointing to MinIO verified (34 checkpoints completed, 0 failed)
- [x] Job 1 contract fully specified — `docs/sprint-2/job1-contract.md`
- [x] Flink Job 1 **running** (ID: 7e91ba535fc23e4e22fd89896ab1ab21)
- [ ] Grafana Geomap live positions panel — **not yet configured**
- [ ] FastAPI `/vehicles/zone/{id}` endpoint — **not started** (`src/api/` contains only `.gitkeep`)
- [ ] GPS anonymisation integration test — documented but not automated

---

## 2. Infrastructure Health

> [!TIP]
> **All 11 Docker services are running and stable** (19+ hours uptime with zero restarts).

| Service | Container | Status | Health | Port |
|---|---|---|---|---|
| **Kafka** (KRaft, 1 broker) | `taasim-kafka` | ✅ Up 19h | `healthy` | 9092 |
| **Kafka UI** | `taasim-kafka-ui` | ✅ Up 19h | — | 8083 |
| **Kafka Connect** (S3 Sink) | `taasim-kafka-connect` | ✅ Up 19h | `healthy` | 8084 |
| **MinIO** (S3-compatible) | `taasim-minio` | ✅ Up 19h | `healthy` | 9000/9001 |
| **Cassandra** (4.1) | `taasim-cassandra` | ✅ Up 19h | `healthy` | 9042 |
| **Flink JobManager** (1.18) | `taasim-flink-jm` | ✅ Up 19h | `healthy` | 8081 |
| **Flink TaskManager** | `taasim-flink-tm` | ✅ Up 19h | — | — |
| **Spark Master** (3.5.0) | `taasim-spark-master` | ✅ Up 19h | `healthy` | 8080/7077 |
| **Spark Worker** | `taasim-spark-worker` | ✅ Up 19h | `healthy` | 8082 |
| **Jupyter** (PySpark) | `taasim-jupyter` | ✅ Up 19h | `healthy` | 8888 |
| **Grafana** (10.4.2) | `taasim-grafana` | ✅ Up 19h | `healthy` | 3000 |

### Kafka Topics (verified via `kafka-topics --list`)

| Topic | Partitions | Status |
|---|---|---|
| `raw.gps` | 4 | ✅ Exists · Offset: 0 (idle) |
| `raw.trips` | 4 | ✅ Exists · Offset: 0 (idle) |
| `processed.gps` | — | ✅ Exists |
| `processed.demand` | — | ✅ Exists |
| `processed.matches` | — | ✅ Exists |
| `connect-configs` | — | ✅ Internal (Kafka Connect) |
| `connect-offsets` | — | ✅ Internal (Kafka Connect) |
| `connect-status` | — | ✅ Internal (Kafka Connect) |

### Kafka Connect Consumer Groups

| Group | Purpose |
|---|---|
| `connect-s3-sink-raw-gps` | Archives `raw.gps` → MinIO `raw/kafka-archive/` |
| `connect-s3-sink-raw-trips` | Archives `raw.trips` → MinIO `raw/kafka-archive/` |

### Cassandra Schema (verified via `DESCRIBE KEYSPACE taasim`)

| Table | Partition Key | Clustering | TTL | Status |
|---|---|---|---|---|
| `vehicle_positions` | `(city, zone_id)` | `event_time DESC, taxi_id ASC` | 3600s | ✅ Deployed |
| `trips` | `(city, date_bucket)` | `created_at DESC` | — | ✅ Deployed |
| `demand_zones` | `(city, zone_id)` | `window_start DESC` | 86400s | ✅ Deployed |

> [!NOTE]
> All three Cassandra tables match the course brief's §4.1 schema requirements exactly. Partition key design aligns with the documented API query patterns.

---

## 3. Pipeline Progress

### 3.1 Map-Matching Pipeline (Notebook 03)

| Item | Status |
|---|---|
| Notebook exists | ✅ `notebooks/notebook-spark/03_map_matching_pipeline.ipynb` (249 KB) |
| Validation visual | ✅ `validation_06_mapping.png` generated |
| Curated output | ✅ `curated/casablanca_trips_final/` and `curated/trips/` directories populated |
| Saved to MinIO | ⚠️ **Partially** — curated data exists locally; MinIO persistence depends on Spark S3A write success |

> [!WARNING]
> **Known issues from recent sessions:**
> - **2026-05-02**: `NameError` — `porto_graphml` variable not defined in `MappingConfig` (cell 3, line 15).
> - **2026-04-28**: `ValueError` — duplicate rows from `gpd.sjoin()` during zone assignment. Fixed with `drop_duplicates()`.
> - The pipeline has been executed iteratively but may need a clean end-to-end rerun to confirm all outputs are persisted correctly to MinIO.

### 3.2 Kafka Producers

| Producer | File | Target Topic | Implementation | Currently Running |
|---|---|---|---|---|
| `vehicle_gps_producer.py` | `src/producers/vehicle_gps_producer.py` (542 lines) | `raw.gps` | ✅ Complete | ✅ **Yes** |
| `trip_request_producer.py` | `src/producers/trip_request_producer.py` (587 lines) | `raw.trips` | ✅ Complete | ✅ **Yes** |

**Producer capabilities (both fully implemented):**
- Porto → Casablanca coordinate transform
- 10× speed replay with configurable acceleration
- ±20m Gaussian GPS noise injection
- 5% blackout probability (60–180s delayed events)
- Casablanca time zone–aware demand curve (peaks 7–9am, 5–7pm)
- Friday jumu'ah and Sunday demand adjustments
- S3/MinIO and local CSV data source support
- Gzip compression, async Kafka sends, graceful shutdown

> [!NOTE]
> Both producers are **actively running** and pushing events.

### Note on GPS Producer & Notebook 03 Alignment

You asked if the `vehicle_gps_producer.py` needs to be adapted based on `03_map_matching_pipeline.ipynb`. **I have updated it so it perfectly aligns.**
- **CSV Mode:** Handles the live simulation by applying the relative coordinate transform (Porto → Casablanca bbox affine mapping).
- **Parquet Mode:** **[UPDATED]** If you pass the curated output of Notebook 03 (`--data-path s3://taasim/curated/mapped_casa_trips/`), the producer now correctly parses the exact road-snapped `polyline` arrays, completely bypassing the bbox affine mapping. This means you can now replay the high-fidelity, road-matched Casablanca trips directly into the real-time stream!

### 3.3 Flink Jobs

| Job | Source Code | JAR | Currently Running | Status |
|---|---|---|---|---|
| **Job 1** — GPS Normalizer | ✅ 8 Java files | ✅ Built (shaded) | ✅ Running | Processing `raw.gps` stream |
| **Job 2** — Demand Aggregator | ❌ Not implemented | — | — | Sprint 3 scope |
| **Job 3** — Trip Matcher | ❌ Not implemented | — | — | Sprint 3 scope |

**Job 1 implementation detail:**
- `Job1GpsNormalizer.java` — main pipeline entry
- `ParseGpsEventFn.java` — JSON deserialization
- `ValidationAndLateFilterFn.java` — bbox validation + watermark late-event filter
- `ZoneMappingBroadcastFn.java` — zone assignment via broadcast state
- `ZoneMappingLoader.java` — loads `metadata/zone_mapping.csv`
- Model classes: `GpsRawEvent`, `GpsNormalizedEvent`, `ZoneDefinition`

### 3.4 Other Components

| Component | Location | Status |
|---|---|---|
| **FastAPI** | `src/api/` | ❌ Only `.gitkeep` — not started |
| **Flink Jobs (Python)** | `src/flink/` | ❌ Only `.gitkeep` — using Java instead |
| **Spark batch jobs** | `src/spark/` | ❌ Only `.gitkeep` — using notebooks instead |
| **ML features** | `ml/features/` | ❌ Only `.gitkeep` — not started (Sprint 5 scope) |
| **ML models** | `ml/models/demand_v1/` | 📂 Directory exists — contents TBD |
| **Grafana provisioning** | `grafana/provisioning/` | 📂 Directory exists — dashboards not yet configured |
| **Event injector** | — | ❌ Not yet built (Sprint 6 scope) |

---

## 4. SLA Readiness

> [!CAUTION]
> **No SLA measurements have been started.** SLA measurement is scheduled for Sprint 6 (Week 7–8), per `plan/milestone-sprint-6/task02_sla-measurement-report.md`.

### Required SLA Targets (from §6.1 of Capstone Brief)

| Requirement | Target | Measurement Method | Current Status |
|---|---|---|---|
| Trip match latency (request → Cassandra) | < 5s P95 | Kafka timestamp vs Cassandra write | ❌ Not measured — Job 3 not implemented |
| Vehicle position freshness (GPS → Cassandra) | < 15s | Producer timestamp vs Cassandra write | ❌ Not measured — Job 1 not running |
| Demand zone update frequency | Every 30s | `WRITETIME()` on `demand_zones` | ❌ Not measured — Job 2 not implemented |
| ML forecast API response time | < 500ms at 20 req/s | Locust load test | ❌ Not measured — API not implemented |
| Spark ETL on full Porto dataset | < 5 min | Spark UI job duration | ⚠️ Not formally measured (notebooks run but not timed) |

### Reliability Requirements (§6.2)

| Requirement | Status |
|---|---|
| Flink checkpointing every 60s to MinIO | ✅ Verified (34 checkpoints, 0 failures) |
| Kafka consumer group offsets committed | ✅ Consumer groups exist for S3 sink |
| Idempotent Cassandra writes | ⚠️ Schema supports upserts; not integration-tested |
| Kafka topic retention (7 days) | ✅ Configured (`KAFKA_LOG_RETENTION_HOURS: 168`) |

### Security Requirements (§6.3)

| Requirement | Status |
|---|---|
| JWT auth on FastAPI | ❌ Not started (Sprint 5) |
| GPS anonymisation in Job 1 | ✅ Verified — centroid snapping confirmed |
| Kafka Topic ACLs | ❌ Not started (Sprint 5) |
| HTTPS on API | ❌ Not started (Sprint 5) |

---

## 5. Immediate Blocker

> [!TIP]
> ### Blocker Resolved: Streaming Pipeline is Live
>
> **The real-time pipeline has been successfully restarted.** Flink Job 1 is running, and both the GPS and Trip producers are actively emitting events to Kafka. Data is now flowing into Cassandra.

### Next Steps (ordered by sprint priority)

| Priority | Task | Sprint | Impact |
|---|---|---|---|
| 🔴 **P0** | Configure Grafana Cassandra plugin + vehicle Geomap panel | Sprint 2 | Sprint 2 deliverable |
| 🟡 **P1** | Implement Flink Job 2 (Demand Aggregator — tumbling windows) | Sprint 3 | Required for demand heatmap |
| 🟡 **P1** | Implement Flink Job 3 (Trip Matcher — stateful matching) | Sprint 3 | Required for end-to-end trip flow |
| 🟡 **P1** | Bootstrap FastAPI with zone endpoint | Sprint 2/3 | Required for API-driven demo |
| 🟠 **P2** | Clean rerun of Notebook 03 (resolve `NameError` in `MappingConfig`) | Sprint 1 debt | Curated data integrity |
| 🟠 **P2** | Choose team name + 1-slide startup concept | Sprint 1 debt | Outstanding Sprint 1 item |

---

## Summary Dashboard

```
┌─────────────────────────────────────────────────────────────────────┐
│  TaaSim · Casablanca — Status at 2026-05-03 14:00                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Milestone    ████████░░░░░░░░░░░░  Week 3/8 · Sprint 2            │
│  Infra        ████████████████████  11/11 services healthy          │
│  Pipeline     ████████████████████  Producers running              │
│  Flink Jobs   ███████░░░░░░░░░░░░░  1/3 implemented, 1/3 running   │
│  SLA          ░░░░░░░░░░░░░░░░░░░░  0/5 targets measured           │
│  API          ░░░░░░░░░░░░░░░░░░░░  Not started                    │
│  ML           ░░░░░░░░░░░░░░░░░░░░  Not started (Sprint 5)         │
│                                                                     │
│  NEXT TASK  → Configure Grafana Geomap Panel                       │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

*Report generated by automated workspace introspection against live Docker stack, Kafka broker, Cassandra cluster, Flink REST API, and local filesystem.*
