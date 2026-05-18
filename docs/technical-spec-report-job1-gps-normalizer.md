# TaaSim Casablanca — Flink Job-1 (GPS Normalizer) Technical Spec Report

Generated: **2026-05-05**

Scope:
- Code inspected: `flink_jobs/src/main/java/com/taasim/flink/job1/` (Job-1 pipeline)
- Live environment inspected: Docker Compose stack + Flink REST API on `http://localhost:8081`

---

## 1) Code Architecture

### 1.1 Java package tree (`com.taasim.flink.job1`)

```text
flink_jobs/src/main/java/com/taasim/flink/job1/
├── Job1GpsNormalizer.java
├── functions/
│   ├── ParseGpsEventFn.java
│   ├── ValidationAndLateFilterFn.java
│   └── ZoneMappingBroadcastFn.java
├── model/
│   ├── GpsNormalizedEvent.java
│   ├── GpsRawEvent.java
│   └── ZoneDefinition.java
└── util/
    └── ZoneMappingLoader.java
```

### 1.2 Responsibilities (high-level)

- **`Job1GpsNormalizer`**: wires the pipeline end-to-end (Kafka source → parsing → watermarks → validation/late drop → zone mapping/anonymization → Cassandra + Kafka sinks), and configures checkpointing + RocksDB state backend.
- **`ParseGpsEventFn`**: parses raw JSON strings into `GpsRawEvent`, with metrics for parse errors and bad events.
- **`ValidationAndLateFilterFn`**: drops invalid events (Casablanca bounding box + speed) and drops late events based on the current watermark; late events go to side output `late_events`.
- **`ZoneMappingBroadcastFn`**: joins GPS events with a broadcasted zone mapping; performs anonymization by snapping GPS coords to the matched zone centroid.
- **`ZoneMappingLoader`**: loads `zone_mapping.csv` from the classpath (sourced from `../metadata/zone_mapping.csv` via Maven resources).

---

## 2) Logic Breakdown

This section is based on the source code in:
- `Job1GpsNormalizer.java`
- `ValidationAndLateFilterFn.java`
- plus the anonymization implementation found in `ZoneMappingBroadcastFn.java` (called by `Job1GpsNormalizer`).

### 2.1 Watermark strategy + allowed lateness (exact)

**Watermark Strategy (exact code behavior)**
- Watermarks are enabled on the parsed `GpsRawEvent` stream via:
  - `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(3))`
  - `withTimestampAssigner((event, ts) -> event.eventTimeMillis)`
- Auto watermark emission interval:
  - `env.getConfig().setAutoWatermarkInterval(1_000L)` (1 second)

**What “allowed lateness” means in this job**
- There is **no window operator** with `.allowedLateness(...)` in Job-1.
- Instead, lateness is enforced explicitly in `ValidationAndLateFilterFn` using the current watermark:
  - `isLate(eventTimeMillis, currentWatermark)` returns `true` when:
    - `currentWatermark != Long.MIN_VALUE` and `eventTimeMillis < currentWatermark`

**Effective lateness threshold**
- The job’s event-time tolerance is driven by the watermark generator:
  - **Bounded out-of-orderness = 3 minutes**
  - Any event with `eventTimeMillis` earlier than the **current watermark** is considered **late** and is **dropped from the main stream**.

**Late-event handling**
- Late events are sent to a side output tag:
  - `ValidationAndLateFilterFn.LATE_EVENTS_TAG` (`"late_events"`)
- In `Job1GpsNormalizer`, this side output is **not consumed** (so late events are effectively dropped unless another operator consumes it later).

### 2.2 Casablanca validation bounding box (exact)

`ValidationAndLateFilterFn` enforces a hard-coded Casablanca bounding box:

| Field | Value |
|---|---:|
| `CASABLANCA_LON_MIN` | `-7.8` |
| `CASABLANCA_LON_MAX` | `-7.4` |
| `CASABLANCA_LAT_MIN` | `33.4` |
| `CASABLANCA_LAT_MAX` | `33.7` |

Validation logic (`isInCasablancaBbox`) uses inclusive checks:
- `lon >= -7.8 && lon <= -7.4 && lat >= 33.4 && lat <= 33.7`

Additional validation in the same function:
- Max speed allowed:
  - `MAX_SPEED_KMH = 150.0f`
  - `isSpeedValid(speedKmh)` returns true iff `speedKmh <= 150.0`

Metrics emitted by this operator:
- `invalid_bbox` (dropped for bbox)
- `speed_too_high` (dropped for speed)
- `dropped_late` (late events)

### 2.3 Anonymization: snapping coordinates to zone centroids (exact)

**Where it happens**
- In `ZoneMappingBroadcastFn.processElement(...)`.

**How zones are defined**
- Zones are loaded from `zone_mapping.csv` using `ZoneMappingLoader.loadZonesFromClasspath()`.
- Each `ZoneDefinition` contains a rectangular bounding box:
  - `lonMin`, `lonMax`, `latMin`, `latMax`
- A point is considered inside a zone using inclusive bounds:
  - `ZoneDefinition.contains(lon, lat)` → `lon >= lonMin && lon <= lonMax && lat >= latMin && lat <= latMax`

**How centroids are computed**
- `ZoneMappingLoader` computes the centroid deterministically:
  - `centroidLon = (lonMin + lonMax) / 2.0`
  - `centroidLat = (latMin + latMax) / 2.0`

**Zone match + snap behavior (exact)**
- For each GPS event, the function iterates through broadcasted zones and selects the **first** zone where `zone.contains(value.lon, value.lat)`.
- If a match is found, the output `GpsNormalizedEvent` is produced with:
  - `normalized.zoneId = matched.arrondissementId`
  - `normalized.lat = matched.centroidLat`
  - `normalized.lon = matched.centroidLon`

This is the anonymization step: the original `(lat, lon)` is replaced by the **zone centroid**.

---

## 3) Connectivity (Kafka + Cassandra)

### 3.1 Kafka source (consumed)

Configured in `Job1GpsNormalizer`:
- Bootstrap servers (param, default):
  - `--kafka-bootstrap-servers`, default `kafka:29092`
- Source topic (param, default):
  - `--source-topic`, default `raw.gps`
- Consumer group id:
  - `flink-job1-gps`
- Starting offsets:
  - `OffsetsInitializer.earliest()`
- Deserialization:
  - `SimpleStringSchema()` (consumes JSON as raw strings)

### 3.2 Kafka sink (produced)

Configured in `Job1GpsNormalizer`:
- Sink topic (param, default):
  - `--sink-topic`, default `processed.gps`
- Delivery semantics:
  - `DeliveryGuarantee.AT_LEAST_ONCE`
- Record serialization:
  - Key: `taxiId` (UTF-8 bytes)
  - Value: `GpsNormalizedEvent.toJson()` (UTF-8 bytes)

### 3.3 Cassandra sink (table updated)

`Job1GpsNormalizer` writes to Cassandra using this exact CQL:

```sql
INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
```

Runtime connection parameters:
- `--cassandra-host` (default `cassandra`)
- `--cassandra-port` (default `9042`)

Schema confirmation:
- `db/cassandra_init.cql` defines `taasim.vehicle_positions` with:
  - primary key `((city, zone_id), event_time, taxi_id)`
  - clustering order `event_time DESC`
  - TTL `default_time_to_live = 3600`

---

## 4) Operational State (Live Flink Environment)

### 4.1 Flink runtime (Docker Compose)

Observed running services (from `docker compose ps`):
- Flink JobManager container: `taasim-flink-jm` (port `8081:8081`, healthy)
- Flink TaskManager container: `taasim-flink-tm` (healthy)

Flink REST endpoint:
- `http://localhost:8081`

Flink image/version (from Docker Compose):
- `flink:1.18.1-scala_2.12-java17`

### 4.2 Job status for Job ID `7e91ba535fc23e4e22fd89896ab1ab21`

**Result:** The Flink REST API reports this job ID as **not found** at the time of inspection.

Evidence (REST):
- `GET /jobs/7e91ba535fc23e4e22fd89896ab1ab21` → `NotFoundException: Job ... not found`

Interpretation:
- This usually means the job is **not currently running** on the cluster and is **not available in the JobManager’s retained history** (e.g., restarted with a new JobID, or archived history purged).

### 4.3 Current running Job-1 instance (for reference)

`GET /jobs/overview` shows one running instance:
- Name: `job1-gps-normalizer`
- JobID (current): `472a09ecf5218af051829d14a67c3a21`
- State: `RUNNING`
- Start time (UTC): `2026-05-04T15:41:32.213Z`
- Last modification (UTC): `2026-05-04T19:00:24.438Z`

### 4.4 Checkpointing evidence (latest successful checkpoint + MinIO path)

#### a) Checkpoint configuration (from code)

`Job1GpsNormalizer` configures:
- `checkpoint-interval-ms` default: `60_000` (60s)
- Mode: `AT_LEAST_ONCE`
- Storage: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`
- Min pause between checkpoints: `30_000` (30s)
- Max concurrent checkpoints: `1`
- State backend: `EmbeddedRocksDBStateBackend`

#### b) Flink REST checkpoint stats (current running JobID)

From `GET /jobs/472a09ecf5218af051829d14a67c3a21/checkpoints`:
- Latest completed checkpoint: **ID 366**
- Trigger time (UTC): `2026-05-05T10:46:52.474Z`
- Completed (ack) time (UTC): `2026-05-05T10:46:52.498Z`
- Externalized path (MinIO / S3A):
  - `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/472a09ecf5218af051829d14a67c3a21/chk-366`

It also reports the job was restored from:
- Checkpoint ID 198
- Restore time (UTC): `2026-05-04T18:59:35.621Z`
- Restore path:
  - `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/472a09ecf5218af051829d14a67c3a21/chk-198`

#### c) Log evidence (JobManager)

Recent JobManager logs show checkpoint completion and S3 commit paths:

```text
2026-05-05 10:46:52,521 INFO  ...CheckpointCoordinator - Completed checkpoint 366 for job 472a09ecf5218af051829d14a67c3a21 ...
2026-05-05 10:46:52,513 INFO  ...S3Committer - Committing raw/kafka-archive/flink-checkpoints/job1/472a09ecf5218af051829d14a67c3a21/chk-366/_metadata ...
```

---

## Appendix A — Input / Output event shapes (for debugging)

### A.1 Expected raw input JSON (`raw.gps`)

`ParseGpsEventFn` expects these fields:
- `taxi_id` (string, required)
- `timestamp` (string, required; must parse with `Instant.parse`, i.e., ISO-8601)
- `lat` (number or numeric string, required)
- `lon` (number or numeric string, required)
- `speed` (number or numeric string, required)
- `status` (string, required)
- `trip_id` (string, optional; defaults to empty string)

### A.2 Produced output JSON (`processed.gps`)

`GpsNormalizedEvent.toJson()` outputs:
- `taxi_id`
- `timestamp` (original if present, else derived from `eventTimeMillis`)
- `lat` / `lon` (zone centroid)
- `speed`
- `status`
- `trip_id`
- `arrondissement_id` (zone id)
