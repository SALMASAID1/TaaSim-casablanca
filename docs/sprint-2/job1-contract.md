# Job 1 (GPS Normalizer) — Contracts (Step 0)

This document is the **single source of truth** for what Job‑1 must read, write, and guarantee.
You will implement Job‑1 in Java (Maven) against these contracts.

## 1) Input contract — Kafka `raw.gps`

**Topic**: `raw.gps`

**Key**: `taxi_id` (string)

**Value**: JSON (no schema registry). Produced by the GPS producer.

### Required fields

| Field | Type | Example | Notes |
|---|---|---|---|
| `taxi_id` | string | `"20000528"` | Used for Kafka keying and Cassandra uniqueness |
| `timestamp` | string (ISO‑8601 UTC) | `"2026-04-20T14:30:12Z"` | Must parse with `Instant.parse()` |
| `lat` | number | `33.592312` | Raw coordinate (must never be persisted) |
| `lon` | number | `-7.612903` | Raw coordinate (must never be persisted) |
| `speed` | number | `42.5` | km/h |
| `status` | string | `"available"` | free-form |
| `trip_id` | string | `"1372636858620000589"` | optional/empty allowed |

### Validations (must happen before any sink)

1. **Casablanca bbox**
   - lon in $[-7.8, -7.4]$
   - lat in $[33.4, 33.7]$
   - If outside: **drop** and increment metric `invalid_bbox`.

2. **Speed sanity**
   - If `speed > 150`: **drop** and increment metric `speed_too_high`.

## 2) Zone mapping contract — `metadata/zone_mapping.csv`

**File**: [metadata/zone_mapping.csv](../../metadata/zone_mapping.csv)

**Columns**:
- `arrondissement_id` (int 1–16)
- `zone_name` (string)
- `lon_min`, `lon_max`, `lat_min`, `lat_max` (bbox)

### Matching rule
A GPS point belongs to a zone if:
- `lon_min <= lon <= lon_max` AND `lat_min <= lat <= lat_max`.

If no zone matches: **drop** and increment metric `zone_not_found`.

### Anonymization rule (centroid)
The stored/sent coordinates MUST be anonymized to the zone centroid.

Centroid is derived from the bbox:
$$
centroid\_lat = \frac{lat_{min}+lat_{max}}{2},\quad centroid\_lon = \frac{lon_{min}+lon_{max}}{2}
$$

## 3) Output contract — Kafka `processed.gps`

**Topic**: `processed.gps`

**Key**: `taxi_id` (string)

**Value**: JSON with the same core fields as `raw.gps`, plus the zone id.

### Required fields

Everything from `raw.gps`, plus:
- `arrondissement_id` (int 1–16)

### Required privacy guarantee
`lat/lon` in `processed.gps` MUST be anonymized centroid coordinates (never the raw input).

## 4) Serving sink contract — Cassandra `taasim.vehicle_positions`

**Table**: `taasim.vehicle_positions`

**Primary key**: `((city, zone_id), event_time, taxi_id)`
- `city` is a constant for this project: `casablanca`
- `event_time` is derived from `timestamp`
- `taxi_id` prevents overwrites when multiple vehicles share the same second timestamp

**Clustering order**: `event_time DESC, taxi_id ASC`

**TTL**: 3600 seconds (1 hour)

### Columns written by Job‑1
- `city` (text)
- `zone_id` (int)
- `event_time` (timestamp)
- `taxi_id` (text)
- `lat` (double) — anonymized
- `lon` (double) — anonymized
- `speed` (float)
- `status` (text)

### Required privacy guarantee
Raw `lat/lon` from `raw.gps` must **never** appear in this table.

## 5) Event-time reliability contract

### Watermarks
- Strategy: bounded out-of-orderness
- Max out-of-orderness: **3 minutes**

### “Too late” rule
If an event timestamp is **older than the current watermark**, it is considered too late:
- **Drop** it
- Increment metric `dropped_late`
- (Optional) side-output to a `late_events` stream for evidence

### Checkpointing
- Mode: `AT_LEAST_ONCE`
- Interval: `60s`
- Storage: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`
- State backend: RocksDB

## 6) Runtime configuration (Java args)

Recommend implementing these CLI args via Flink `ParameterTool`:
- `--kafka-bootstrap-servers` (default `kafka:29092`)
- `--source-topic` (default `raw.gps`)
- `--sink-topic` (default `processed.gps`)
- `--cassandra-host` (default `cassandra`)
- `--cassandra-port` (default `9042`)
- `--city` (default `casablanca`)
- `--checkpoint-dir` (default `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`)
- `--checkpoint-interval-ms` (default `60000`)

## 7) Definition of done (Sprint‑2)

Job‑1 is “done” when:
- It runs 10 minutes without exceptions
- It writes anonymized rows to Cassandra
- It forwards anonymized events to `processed.gps`
- Watermark tests (2 min late accepted, 4 min late dropped) are evidenced in the Sprint‑2 docs
- A test confirms raw coordinates are never persisted