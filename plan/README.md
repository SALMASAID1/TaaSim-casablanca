# TaaSim — Execution Plan

**Transport as a Service · Urban Mobility Platform · Casablanca, Morocco**  
Advanced Big Data — Final Capstone · ENSA Al Hoceima · 2025–2026

> *"Build the data platform that moves Casablanca."*  
> You are not just students. You are co-founders.

---

## Project Overview

| Field | Detail |
|---|---|
| Course | Advanced Big Data — Final Capstone |
| School | National School of Applied Sciences — Al Hoceima (ENSAH) |
| Duration | 8 weeks (weekly labs) mapped to 6 sprints |
| Team | 2 co-founders working in parallel |
| Tech Stack | Kafka · Spark · Flink · MinIO · Cassandra · Grafana · FastAPI |
| Datasets | Porto Taxi Trajectories (ECML 2015) + NYC TLC (Kaggle) |
| Demo Framing | Seed-round investor pitch — 20 min demo + 10 min Q&A |

---

## How to Use This Plan

Each sprint maps to a **milestone folder** inside the `plan/` directory.  
Inside every milestone folder there is one `.md` file per discrete task.  
Each file contains: **Context · Objective · Acceptance Criteria · Technical Hints · Status**.

**Naming convention:** tasks are numbered `01`, `02`, … (zero-padded). The slug after the
underscore is a short kebab-case description. Extension is always `.md`.

**Founder colour coding:**
- 🔵 **Founder A** — Infrastructure, Streaming, Batch, Security
- 🟣 **Founder B** — Data Engineering, Producers, API, Grafana, ML

---

## Alignment With Official Course Brief (ID2-ABD Capstone 2026)

This plan follows the official brief in `plan/ID2-ABD-Capstone project-2026.txt`.
The course is structured as an 8-week lab plan; we map it into 6 sprints for execution:

- **Weeks 1–2** → **Sprint 1** (stack + datasets + remapping + producers + storage design + ADR)
- **Week 3** → **Sprint 2** (Flink Job 1 + watermarks + checkpointing + Grafana vehicle map + API bootstrap)
- **Week 4** → **Sprint 3** (Flink Job 2/3 + demand heatmap + trip matching + fallback)
- **Week 5** → **Sprint 4** (Spark ETL + KPIs)
- **Weeks 6–7** → **Sprint 5** (ML + JWT + Kafka ACLs + HTTPS)
- **Week 8** → **Sprint 6** (SLA measurements + checkpoint recovery + report + pitch)

---

## Demo Day Non-Negotiables (5 things that must work)

1. ✅ GPS events: `Kafka → Flink Job 1 → Cassandra → Grafana` vehicle map (live)
2. ✅ Trip reservation: `POST /api/v1/trips → Flink Job 3 match → Cassandra` ETA < 5 s
3. ✅ Demand heatmap: `Flink Job 2 → Cassandra demand_zones → Grafana` every 30 s
4. ✅ ML forecast: `POST /api/v1/demand/forecast` responds in < 500 ms
5. ✅ Anomaly visible: `event_injector.py` demand spike → heatmap surge within 60 s

---

## Full Task Index

### Sprint 1 — Foundation & Data Mapping
`plan/milestone-sprint-1/`  
*Goal: Deploy the full Big Data stack and transform Porto GPS data to Casablanca coordinates.*

| File | Assigned To | Description |
|---|---|---|
| [task01_docker-compose-stack.md](milestone-sprint-1/task01_docker-compose-stack.md) | Founder A | Provision Docker Compose stack: Kafka, MinIO, Cassandra, Flink, Spark, Grafana |
| [task02_minio-bucket-structure.md](milestone-sprint-1/task02_minio-bucket-structure.md) | Founder A | Create MinIO bucket layout: `raw/`, `curated/`, `ml/`, `raw/kafka-archive/` |
| [task03_service-connectivity-config.md](milestone-sprint-1/task03_service-connectivity-config.md) | Founder A | Configure S3A connector for Flink and Spark to read/write MinIO |
| [task04_porto-casablanca-coordinate-transform.md](milestone-sprint-1/task04_porto-casablanca-coordinate-transform.md) | Founder B | PySpark linear coordinate transform Porto bbox → Casablanca bbox |
| [task05_vehicle-gps-producer.md](milestone-sprint-1/task05_vehicle-gps-producer.md) | Founder B | Build `vehicle_gps_producer.py`: replay Porto polylines at 10× with GPS noise |
| [task06_cassandra-keyspace-init.md](milestone-sprint-1/task06_cassandra-keyspace-init.md) | Founder B | Create `taasim` keyspace with `vehicle_positions`, `trips`, `demand_zones` tables |
| [task07_kafka-connect-s3-archive.md](milestone-sprint-1/task07_kafka-connect-s3-archive.md) | Founder A | Kafka → MinIO archival: mirror raw topics to `raw/kafka-archive/` |
| [task08_trip-request-producer.md](milestone-sprint-1/task08_trip-request-producer.md) | Founder B | Build `trip_request_producer.py`: reservation events on `raw.trips` |

**Sprint 1 Checklist**
- [x] Docker stack screenshot (all services green) — `docs/sprint-1/stack-health.png`
- [x] Jupyter data-profiling notebook committed — `notebooks/notebook-spark/01_data_exploration.ipynb`
- [x] Zone-remapped trips visualised on Casablanca map — `docs/sprint-1/casablanca-coordinate-validation.png`
- [x] GPS + trip events flowing in Kafka (`raw.gps`, `raw.trips`) — evidenced via MinIO archive in `docs/sprint-1/kafka-connect-s3-archive.md`
- [x] MinIO `raw/kafka-archive/` receives Kafka topic mirrors (Kafka Connect S3 Sink)
- [x] ADR submitted (storage + Cassandra partition key rationale) — `docs/adr/adr-001-cassandra-schema.md`
- [ ] Team name chosen + 1-slide startup concept

---

### Sprint 2 — Real-Time GPS Normalisation
`plan/milestone-sprint-2/`  
*Goal: Process live GPS pings through Flink Job 1 and visualise anonymised positions on Grafana.*

| File | Assigned To | Description |
|---|---|---|
| [task01_flink-job1-gps-normalizer.md](milestone-sprint-2/task01_flink-job1-gps-normalizer.md) | Founder A | Flink Job 1: validate, zone-map, anonymise GPS events; sink to Cassandra + `processed.gps` |
| [task02_watermark-and-checkpointing.md](milestone-sprint-2/task02_watermark-and-checkpointing.md) | Founder A | BoundedOutOfOrderness watermarks (3 min), checkpointing every 60 s to MinIO |
| [task03_grafana-cassandra-plugin.md](milestone-sprint-2/task03_grafana-cassandra-plugin.md) | Founder B | Install Cassandra plugin; create live Geomap vehicle position panel |
| [task04_fastapi-boilerplate-zone-endpoint.md](milestone-sprint-2/task04_fastapi-boilerplate-zone-endpoint.md) | Founder B | Bootstrap FastAPI; implement `GET /api/v1/vehicles/zone/{zone_id}` |
| [task05_gps-anonymization-verification.md](milestone-sprint-2/task05_gps-anonymization-verification.md) | Founder B | Integration test: confirm raw lat/lon never written to Cassandra |

**Sprint 2 Checklist**
- [ ] Flink Job 1 running with checkpointing enabled
- [ ] Grafana Geomap shows live positions updating ≤ 10 s
- [ ] Late-event watermark test documented
- [ ] `/vehicles/zone/{id}` endpoint responding correctly

---

### Sprint 3 — The Matchmaker & Heatmaps
`plan/milestone-sprint-3/`  
*Goal: Automate trip matching via Flink Job 3 and visualise supply/demand ratios on the heatmap.*

| File | Assigned To | Description |
|---|---|---|
| [task01_flink-job2-demand-aggregator.md](milestone-sprint-3/task01_flink-job2-demand-aggregator.md) | Founder A | Flink Job 2: 30-second tumbling window per zone, compute supply/demand ratio |
| [task02_flink-job3-trip-matcher.md](milestone-sprint-3/task02_flink-job3-trip-matcher.md) | Founder A | Flink Job 3: stateful trip matching, ETA computation, RocksDB state backend |
| [task03_grafana-demand-heatmap.md](milestone-sprint-3/task03_grafana-demand-heatmap.md) | Founder B | Grafana Geomap heatmap panel: colour intensity = demand ratio per zone |
| [task04_zone-adjacent-fallback.md](milestone-sprint-3/task04_zone-adjacent-fallback.md) | Founder B | Job 3 fallback: expand search to adjacent zones if same-zone empty within 5 s |

**Sprint 3 Checklist**
- [ ] End-to-end: reserve → match → ETA < 5 s (measured)
- [ ] Grafana demand heatmap updating every 30 s
- [ ] Flink Job 3 RocksDB state backend configured
- [ ] Adjacent-zone fallback unit-tested

---

### Sprint 4 — Large-Scale ETL & Analytics
`plan/milestone-sprint-4/`  
*Goal: Process Porto 1.7 M rows and NYC TLC 30 M rows with Spark; populate Grafana KPI panel.*

| File | Assigned To | Description |
|---|---|---|
| [task01_spark-etl-porto-dataset.md](milestone-sprint-4/task01_spark-etl-porto-dataset.md) | Founder A | Spark ETL on Porto CSV: parse POLYLINE, zone-remap, deduplicate, write Parquet |
| [task02_spark-etl-nyc-tlc-dataset.md](milestone-sprint-4/task02_spark-etl-nyc-tlc-dataset.md) | Founder A | Spark ETL on NYC TLC: per-zone-per-hour demand aggregates; 10 M rows/month |
| [task03_weekly-kpi-computation.md](milestone-sprint-4/task03_weekly-kpi-computation.md) | Founder B | Spark SQL: weekly KPIs — trips/zone, avg duration, peak hours, coverage gaps |
| [task04_grafana-kpi-table-panel.md](milestone-sprint-4/task04_grafana-kpi-table-panel.md) | Founder B | Grafana KPI table panel: total trips, avg ETA, % SLA matched, top-3 zones |

**Sprint 4 Checklist**
- [ ] Porto ETL < 5 min (Spark UI screenshot)
- [ ] NYC ETL processes ≥ 10 M rows/month
- [ ] Grafana KPI panel showing corridor demand and peak hours
- [ ] `curated/porto-trips/` Parquet readable by Spark ML job

---

### Sprint 5 — Intelligence & Security Hardening
`plan/milestone-sprint-5/`  
*Goal: Deploy GBT demand forecasting model and secure the platform with JWT auth and Kafka ACLs.*

| File | Assigned To | Description |
|---|---|---|
| [task01_fastapi-jwt-authentication.md](milestone-sprint-5/task01_fastapi-jwt-authentication.md) | Founder A | JWT auth on all FastAPI endpoints: rider and admin roles via `python-jose` |
| [task02_kafka-topic-acls.md](milestone-sprint-5/task02_kafka-topic-acls.md) | Founder A | Kafka Topic ACLs: producers → `raw.*` only; Flink → `raw.*` read / `processed.*` write |
| [task03_spark-ml-feature-engineering.md](milestone-sprint-5/task03_spark-ml-feature-engineering.md) | Founder B | Spark feature engineering: temporal, spatial, weather, and lag features |
| [task04_gbt-model-training-validation.md](milestone-sprint-5/task04_gbt-model-training-validation.md) | Founder B | Train GBTRegressor, temporal split, beat naive baseline, save PipelineModel |
| [task05_fastapi-demand-forecast-endpoint.md](milestone-sprint-5/task05_fastapi-demand-forecast-endpoint.md) | Founder B | `POST /api/v1/demand/forecast`: load model at startup, respond < 500 ms |
| [task06_fastapi-https.md](milestone-sprint-5/task06_fastapi-https.md) | Founder A | Enable HTTPS on FastAPI (self-signed cert acceptable for demo) |

**Sprint 5 Checklist**
- [ ] JWT auth working on all endpoints
- [ ] Model RMSE beats naive 7-day-lag baseline (per-zone comparison table)
- [ ] Feature importance chart explaining top 3 predictors
- [ ] `POST /api/v1/demand/forecast` responds in < 500 ms (Locust result)
- [ ] HTTPS enabled on FastAPI (self-signed cert acceptable)
- [ ] Kafka ACLs verified

---

### Sprint 6 — Demo Day & Investor Pitch
`plan/milestone-sprint-6/`  
*Goal: Finalise the platform for the seed-round pitch; demonstrate resilience and submit all deliverables.*

| File | Assigned To | Description |
|---|---|---|
| [task01_flink-checkpoint-recovery-test.md](milestone-sprint-6/task01_flink-checkpoint-recovery-test.md) | Founder A | Kill Task Manager mid-stream; verify Job 1 recovers from MinIO checkpoint |
| [task02_sla-measurement-report.md](milestone-sprint-6/task02_sla-measurement-report.md) | Founder A | Measure all 5 SLA targets from §6.1 under simultaneous load; record table |
| [task03_event-injector-demand-surge.md](milestone-sprint-6/task03_event-injector-demand-surge.md) | Founder B | Build `event_injector.py` (spike/blackout/rain); rehearse full demo script |
| [task04_pitch-deck-10-slides.md](milestone-sprint-6/task04_pitch-deck-10-slides.md) | Founder B | 10-slide investor pitch deck; rehearsed to 20 minutes |
| [task05_technical-report-15-pages.md](milestone-sprint-6/task05_technical-report-15-pages.md) | Both | 12–15 page technical report with architecture, ML evaluation, ADRs, post-mortem |

**Sprint 6 Demo Day Checklist**
- [ ] GPS events: Kafka → Flink Job 1 → Cassandra → Grafana map (live)
- [ ] Trip reservation: `POST /api/v1/trips` → match in Cassandra with ETA < 5 s
- [ ] Demand heatmap: Job 2 → Cassandra → Grafana updating every 30 s
- [ ] ML forecast: `POST /api/v1/demand/forecast` responds in < 500 ms
- [ ] Anomaly visible: demand spike → heatmap surge within 60 s
- [ ] Checkpoint recovery demonstrated (screen recording)
- [ ] Technical report submitted (12–15 pages)
- [ ] Pitch deck submitted (10 slides)

---

## Key Resources

| Resource | URL |
|---|---|
| Porto Taxi Dataset | https://www.kaggle.com/c/pkdd-15-predict-taxi-service-trajectory-i |
| Porto Dataset (UCI mirror) | https://archive.ics.uci.edu/ml/datasets/Taxi+Service+Trajectory |
| NYC TLC Trip Records | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| OpenStreetMap Morocco | https://download.geofabrik.de/africa/morocco.html |
| Open-Meteo Historical API | https://open-meteo.com/en/docs/historical-weather-api |
| Morocco Open Data | https://data.gov.ma |
| Flink Documentation | https://flink.apache.org/docs/stable/ |
| Cassandra Data Modelling | https://cassandra.apache.org/doc/latest/cassandra/data_modeling/ |

---

*TaaSim · Advanced Big Data Capstone · ENSA Al Hoceima · 2025–2026*  
*"The best time to build the data infrastructure for Moroccan mobility was 10 years ago. The second best time is now."*
