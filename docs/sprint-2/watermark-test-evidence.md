# Watermark & Checkpointing Evidence (Sprint 2)

## Goal
Demonstrate **event-time processing** with a **3-minute allowed lateness watermark** and **Flink checkpointing to MinIO**.

## Setup
- Flink Job 1 deployed and running
- Checkpointing interval: 60s
- Checkpoint storage: MinIO (S3A)
- Checkpoint directory: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`

## Test Cases

### Case A — Late by 2 minutes (should be processed)
- Inject a GPS event with `timestamp = now - 2 minutes`
- Expected: event is processed and appears in Cassandra

### Case B — Late by 4 minutes (should be dropped)
- Inject a GPS event with `timestamp = now - 4 minutes`
- Expected: event is dropped and the late-event counter increments

## Evidence

### Run (2026-04-21) — Try Again

#### Job
- Job name: `job1-gps-normalizer`
- Job ID (RUNNING): `36f54e632ce9db664d2ea9e266492573`

#### Test IDs
- run_id: `20260421T181658Z`
- taxi_id (key): `wm_tryagain_20260421T181658Z`

#### Notes (partitioned topic)
`raw.gps` has **4 partitions**. Watermarks are computed per-partition and the effective watermark is the **minimum** across partitions.

To make the lateness test deterministic, we first advanced event-time on **all partitions** using one “base timestamp” record per partition. For non-test partitions we used `speed=200` so the record is filtered by validation (not persisted to Cassandra) while still advancing watermark (timestamp assignment happens before validation).

#### Commands (repro)

```bash
# Upload JAR (fresh Flink restart)
curl -sf -X POST -H 'Expect:' -F 'jarfile=@flink_jobs/target/taasim-flink-jobs-1.0.0-shaded.jar' \
	http://localhost:8081/jars/upload

# Run Job-1 (this run)
curl -sS -X POST -H 'Content-Type: application/json' \
	-d '{"entryClass":"com.taasim.flink.job1.Job1GpsNormalizer"}' \
	http://localhost:8081/jars/79dc5214-ea24-40ae-8ed9-d75517144468_taasim-flink-jobs-1.0.0-shaded.jar/run

# Kafka partition count
docker exec -i taasim-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic raw.gps

# Timestamps used (derived from current watermark at test start)
# TS_BASE=2026-04-21T17:41:59Z
# TS_L2  =2026-04-21T17:39:59Z
# TS_L4  =2026-04-21T17:37:59Z

# BASE record for the test key (partition 1)
python3 -c 'import json; key="wm_tryagain_20260421T181658Z"; ts="2026-04-21T17:41:59Z"; evt={"taxi_id":key,"trip_id":"base","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":30.0,"status":"available"}; print(key+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Advance other partitions (keys chosen to map to partitions 0/2/3) — invalid speed so they do not persist
python3 -c 'import json; ts="2026-04-21T17:41:59Z"; 
keys=["wm_adv_2","wm_adv_3","wm_adv_0"]; 
for k in keys:
  evt={"taxi_id":k,"trip_id":"adv","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":200.0,"status":"available"};
  print(k+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Case A: late by 2 minutes (should be processed)
python3 -c 'import json; key="wm_tryagain_20260421T181658Z"; ts="2026-04-21T17:39:59Z"; evt={"taxi_id":key,"trip_id":"late2m","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":30.0,"status":"available"}; print(key+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Case B: late by 4 minutes (should be dropped)
python3 -c 'import json; key="wm_tryagain_20260421T181658Z"; ts="2026-04-21T17:37:59Z"; evt={"taxi_id":key,"trip_id":"late4m","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":30.0,"status":"available"}; print(key+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Metric evidence
curl -sS \
	"http://localhost:8081/jobs/36f54e632ce9db664d2ea9e266492573/vertices/cbc357ccb763df2852fee8c4fc7d55f2/metrics?get=0.validate-and-late-filter.dropped_late"
```

#### Cassandra (Case A persisted, Case B absent)

```sql
SELECT event_time, taxi_id, speed, status
FROM taasim.vehicle_positions
WHERE city='casablanca' AND zone_id=1 AND taxi_id='wm_tryagain_20260421T181658Z'
ALLOW FILTERING;
```

```text
 event_time                      | taxi_id                      | speed | status
---------------------------------+------------------------------+-------+-----------
 2026-04-21 17:41:59.000000+0000 | wm_tryagain_20260421T181658Z |    30 | available
 2026-04-21 17:39:59.000000+0000 | wm_tryagain_20260421T181658Z |    30 | available

(2 rows)
```

The 4-minute-late record at `2026-04-21T17:37:59Z` is absent in Cassandra.

#### Flink metric (Case B dropped_late incremented)

```json
[{"id":"0.validate-and-late-filter.dropped_late","value":"1"}]
```

#### Checkpointing (Flink REST + MinIO)

Flink REST checkpoint summary:

```text
counts= {'restored': 0, 'total': 34, 'in_progress': 0, 'completed': 34, 'failed': 0}
latest_completed_external_path= s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/36f54e632ce9db664d2ea9e266492573/chk-34
latest_completed_status= COMPLETED
```

MinIO listing (filesystem view inside container):

```text
/data/taasim/raw/kafka-archive/flink-checkpoints/job1/36f54e632ce9db664d2ea9e266492573:
chk-34
shared__XLDIR__
taskowned__XLDIR__
```

### Run (2026-04-20)

#### Job
- Job name: `job1-gps-normalizer`
- Job ID (RUNNING): `fab713a45dc5ddd3c1ffb4522375ca9c`

#### Commands (repro)

```bash
# Build shaded JAR
cd flink_jobs
mvn -q -DskipTests package

# Upload + run on Flink
curl -sf -X POST -H 'Expect:' -F 'jarfile=@flink_jobs/target/taasim-flink-jobs-1.0.0-shaded.jar' \
	http://localhost:8081/jars/upload

curl -sf -X POST \
	"http://localhost:8081/jars/<jar-id>/run?entry-class=com.taasim.flink.job1.Job1GpsNormalizer"

# Inject baseline + late events
python3 -c 'import json,datetime; now=datetime.datetime.now(datetime.timezone.utc); msg={"taxi_id":"wm_base_001","timestamp":now.isoformat().replace("+00:00","Z"),"lat":33.600,"lon":-7.610,"speed":35.0,"status":"AVAILABLE","trip_id":"trip_test_base"}; print(json.dumps(msg))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic raw.gps

python3 -c 'import json,datetime; now=datetime.datetime.now(datetime.timezone.utc); base={"lat":33.600,"lon":-7.610,"speed":35.0,"status":"AVAILABLE"};
for taxi_id,delta,trip in [("wm_late_2m_001",120,"trip_test_2m"),("wm_late_4m_001",240,"trip_test_4m")]:
	ts=(now-datetime.timedelta(seconds=delta)).isoformat().replace("+00:00","Z");
	msg={"taxi_id":taxi_id,"timestamp":ts,**base,"trip_id":trip};
	print(json.dumps(msg))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic raw.gps

# Metric evidence
curl -s \
	"http://localhost:8081/jobs/fab713a45dc5ddd3c1ffb4522375ca9c/vertices/cbc357ccb763df2852fee8c4fc7d55f2/metrics?get=0.validate-and-late-filter.dropped_late"

# MinIO listing evidence
docker run --rm --network taasim-casablanca_taasim \
	-e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
	minio/mc ls minio/taasim/raw/kafka-archive/flink-checkpoints/job1/fab713a45dc5ddd3c1ffb4522375ca9c/
```

#### Kafka → processed.gps (shows Case A output exists)

Observed messages in `processed.gps` for the test taxi IDs:

```text
wm_base_001|{"taxi_id":"wm_base_001","timestamp":"2026-04-20T21:18:25.044713Z","lat":33.605000000000004,"lon":-7.609999999999999,"speed":35.0,"status":"AVAILABLE","trip_id":"trip_test_base","arrondissement_id":1}
wm_late_2m_001|{"taxi_id":"wm_late_2m_001","timestamp":"2026-04-20T21:17:08.247876Z","lat":33.605000000000004,"lon":-7.609999999999999,"speed":35.0,"status":"AVAILABLE","trip_id":"trip_test_2m","arrondissement_id":1}
```

#### Cassandra (Case A persisted, Case B absent)

Query used:

```sql
SELECT event_time, taxi_id, lat, lon, speed, status
FROM taasim.vehicle_positions
WHERE city='casablanca' AND zone_id=1
LIMIT 20;
```

Output (note centroid anonymization: input lat=33.600/lon=-7.610 → stored lat=33.605/lon=-7.610):

```text
 event_time                      | taxi_id        | lat    | lon   | speed | status
---------------------------------+----------------+--------+-------+-------+-----------
 2026-04-20 21:18:25.044000+0000 |    wm_base_001 | 33.605 | -7.61 |    35 | AVAILABLE
 2026-04-20 21:17:08.247000+0000 | wm_late_2m_001 | 33.605 | -7.61 |    35 | AVAILABLE

(2 rows)
```

`wm_late_4m_001` is not present in Cassandra, consistent with being dropped as late.

#### Flink metric (Case B dropped_late incremented)

```json
[{"id":"0.validate-and-late-filter.dropped_late","value":"1"}]
```

#### Checkpointing (Flink REST + MinIO)

Flink REST checkpoint stats:

```text
counts= {'restored': 0, 'total': 30, 'in_progress': 0, 'completed': 30, 'failed': 0}
latest_completed= {... 'status': 'COMPLETED', ... 'external_path': 's3a://taasim/raw/kafka-archive/flink-checkpoints/job1/fab713a45dc5ddd3c1ffb4522375ca9c/chk-30', ...}
```

MinIO listing (via `mc`):

```text
s3://taasim/raw/kafka-archive/flink-checkpoints/job1/
	fab713a45dc5ddd3c1ffb4522375ca9c/
		chk-29/
		shared/
		taskowned/

s3://taasim/raw/kafka-archive/flink-checkpoints/job1/fab713a45dc5ddd3c1ffb4522375ca9c/chk-29/
	_metadata
```

### Checklist
- [ ] (Optional) Flink UI screenshot showing checkpoints completed (open http://localhost:8081 → Job → Checkpoints)
- [x] MinIO listing showing checkpoint directory populated
- [x] Evidence of Case A processed (Cassandra + processed.gps)
- [x] Evidence of Case B dropped (metric `dropped_late=1` + Cassandra absence)
