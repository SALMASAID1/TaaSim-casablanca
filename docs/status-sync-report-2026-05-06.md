# TaaSim · Casablanca — Status Sync Report (v2, evidence-based)

**Report Date:** 2026-05-06 03:53 (UTC+01:00)

**Scope:** This report is based on (a) repo/workspace inspection and (b) live stack checks against the currently running Docker Compose environment.

---

## 0. Evidence Snapshot (what was verified)

**Live stack (Docker Compose):** 11 services are up; most are `healthy`.

**Flink (REST):** Job **`job1-gps-normalizer`** is **RUNNING**.
- Job ID: `68161f6bd7500b23b7091b446f47a8da`
- Checkpoints: **55 completed**, **0 failed**
- Externalized checkpoint path: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/68161f6bd7500b23b7091b446f47a8da/chk-55`

**Kafka (broker CLI):**
- Topics exist: `raw.gps`, `raw.trips`, `processed.gps`, `processed.demand`, `processed.matches` (+ Connect internals)
- `raw.gps`: 4 partitions, retention 7 days, end offsets are **non-zero** (topic contains data)
- `raw.trips`: 4 partitions, end offsets are **0** (no trip events observed in broker)
- `processed.gps`: end offsets are **non-zero** (Job 1 produces output)
- `processed.demand`, `processed.matches`: end offsets are **0** (Jobs 2/3 not producing)

**Kafka Connect (REST):** connectors are deployed and **RUNNING**:
- `s3-sink-raw-gps` (RUNNING)
- `s3-sink-raw-trips` (RUNNING)

**Cassandra (cqlsh):** keyspace `taasim` exists with 3 tables:
- `vehicle_positions` (TTL 3600)
- `trips`
- `demand_zones` (TTL 86400)

**Cassandra data:** `taasim.vehicle_positions` contains rows (Job 1 is writing).

**MinIO (container filesystem view):** bucket `taasim` exists, and prefixes exist for:
- `raw/kafka-archive/` (including Flink checkpoints)
- `curated/mapped_casa_trips/` (curated parquet present)

**Grafana:** Cassandra datasource plugin is installed: `hadesarchitect-cassandra-datasource@3.2.0`.

---

## 1. Current Milestone (plan mapping)

Based on the plan mapping in `plan/README.md` and the previous status report dated 2026-05-03:
- Sprint 1 (Weeks 1–2): **Foundation & Data Mapping** — ✅ done (evidence committed under `docs/sprint-1/`)
- Sprint 2 (Week 3): **Real-Time GPS Normalisation** — ✅ mostly complete (see Sprint 2 status below)
- Sprint 3 (Week 4): **Job 2/3 + Heatmaps + Matching** — 🔶 ready to start

> If your course calendar differs, adjust the week labels; the *technical* status below is evidence-based.

---

## 2. Sprint Status

### Sprint 1 — Foundation & Data Mapping ✅ COMPLETE (repo evidence)

Evidence present in `docs/sprint-1/`:
- `stack-health.png`, `stack-health.txt`
- `casablanca-coordinate-validation.png`
- `kafka-connect-s3-archive.md`
- `minio-layout.md`
- `s3a-connector-setup.md`

### Sprint 2 — Real-Time GPS Normalisation ✅ DELIVERABLES MOSTLY MET

**Confirmed done (repo + live checks):**
- Flink Job 1 implemented under `flink_jobs/src/main/java/com/taasim/flink/job1/` (8 Java files)
- Shaded jar present: `flink_jobs/target/taasim-flink-jobs-1.0.0-shaded.jar`
- Flink Job 1 running (see Evidence Snapshot) with checkpointing to MinIO (`s3a://.../flink-checkpoints/...`)
- Watermark/checkpointing notes exist: `docs/sprint-2/watermark-test-evidence.md`
- Anonymization verification notes exist: `docs/sprint-2/security-verification.md`
- Grafana provisioning present:
  - Datasource: `grafana/provisioning/datasources/cassandra.yaml`
  - Dashboard provisioning: `grafana/provisioning/dashboards/default.yaml`
  - Dashboard JSON: `grafana/dashboards/taasim-live.json` (contains a Geomap panel + Cassandra queries)

**Not confirmed / still pending:**
- FastAPI `/api/v1/vehicles/zone/{zone_id}` endpoint is **not implemented** (`src/api/` contains only `.gitkeep`).
- Trip stream: `raw.trips` currently has **0** messages in Kafka (trip producer likely not running / not exercised).

---

## 3. Infrastructure Health (live)

**Services up (docker compose ps):**
- Kafka, Kafka UI, Kafka Connect
- MinIO
- Cassandra
- Flink JobManager + TaskManager
- Spark Master + Worker
- Jupyter
- Grafana

All are currently up; most report `healthy`.

---

## 4. Dataflow Reality Check (what is flowing right now)

### 4.1 GPS stream (✅ working)

Observed end-to-end chain:
- Kafka `raw.gps` has data (end offsets ~1.7k–2.2k per partition at report time)
- Flink Job 1 is RUNNING and producing to:
  - Cassandra `taasim.vehicle_positions` (rows observed)
  - Kafka `processed.gps` (end offsets ~950–1245 per partition at report time)
- Kafka Connect S3 sink for `raw.gps` is RUNNING and has small lag (tens of messages) at report time

### 4.2 Trips stream (⚠️ not active)

- Kafka `raw.trips` end offsets are **0** (no events observed).
- The S3 sink connector for `raw.trips` is RUNNING, but it has nothing to archive yet.

---

## 5. Alignment Findings (things currently *not* aligned)

These are inconsistencies between the repo + live stack and the older status report `docs/status-sync-report.md`:

- **Stack uptime:** older report says ~19h; current stack shows containers created ~3h ago.
- **Kafka offsets:** older report lists `raw.gps` offsets as 0; broker currently shows **non-zero** offsets.
- **Producers running:** older report states both GPS and trip producers are running; broker shows **GPS topic populated** but **trip topic empty**.
- **Sprint 2 Grafana:** older report lists Geomap panel “not configured”; repo contains provisioning + installed plugin + dashboard JSON.
- **Plan checklists:** several plan task “Acceptance Criteria” checkboxes remain unchecked even where implementation/evidence exists (e.g., Job 1, Grafana plugin).

---

## 6. Next Steps (priority)

**P0 (finish Sprint 2 cleanly):**
1. Implement FastAPI service and the `/api/v1/vehicles/zone/{zone_id}` endpoint.
2. Run/validate trip producer so `raw.trips` has events; confirm the S3 sink writes `raw/kafka-archive/raw.trips/`.
3. Open Grafana UI and confirm the provisioned dashboard panels render and auto-refresh correctly (live dots update).

**P1 (start Sprint 3):**
4. Implement Flink Job 2 (demand aggregation) writing to `demand_zones` + `processed.demand`.
5. Implement Flink Job 3 (trip matcher) writing to `trips` + `processed.matches`.

**P2 (documentation hygiene):**
6. Update plan task checkboxes to reflect reality and link to evidence files.
7. Replace the older status report narrative with a strictly evidence-based template (or keep both as v1/v2).
