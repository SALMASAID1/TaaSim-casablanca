# task03 — Service Connectivity Config

## Context
Flink and Spark both need to read from and write to MinIO using the S3A filesystem connector.
This is not automatic — it requires the correct JARs, environment variables, and configuration
files to be present inside the containers. Getting this wrong causes cryptic `ClassNotFoundException`
or `UnknownHostException` errors in later sprints. Solving it once here prevents all future
debugging.

## Objective
Configure the S3A connector for both Flink and Spark so that both engines can read from and write
to MinIO using `s3a://taasim/...` paths, and verify with a round-trip read/write test.

## Acceptance Criteria
- [x] Flink job can write a test file to `s3a://taasim/raw/test-flink-write/` using `FileSink`
- [x] Spark job can read `s3a://taasim/raw/porto-trips/` and print schema without errors
- [x] Spark job can write a Parquet file to `s3a://taasim/curated/test-spark-write/`
- [x] No `ClassNotFoundException` for `S3AFileSystem` in either engine's logs
- [x] Configuration documented in `docs/sprint-1/s3a-connector-setup.md` with exact JAR versions used

## Technical Hints
- Required JARs (add to Flink's `lib/` folder and Spark's `jars/` folder):
  - `hadoop-aws-3.3.x.jar`
  - `aws-java-sdk-bundle-1.12.x.jar`
  These are available via Maven Central. Pin the version to match your Hadoop distribution.
- Flink `flink-conf.yaml` additions:
  ```yaml
  s3.access-key: minioadmin
  s3.secret-key: minioadmin
  s3.endpoint: http://minio:9000
  s3.path.style.access: true
  ```
- Spark `spark-defaults.conf` additions (see task02 for full list).
- Kafka Connect S3 Sink also needs S3A config — set `store.url=http://minio:9000` in the connector
  JSON when configuring in Sprint 1 (Kafka Connect worker must have `aws-java-sdk-bundle` in its
  plugin path).
- Reference: project brief §9.1 Infrastructure Setup, §3.1 Stack Overview.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
