# task01 — Docker Compose Stack

## Context
Sprint 1 goal is to have a fully operational Big Data environment before any data engineering work
begins. Without a running stack, neither founder can progress. This task provisions every service
TaaSim depends on — Kafka, MinIO, Cassandra, Flink, Spark, and Grafana — via a single
`docker-compose.yml` so the environment is reproducible on any workstation with 8 GB RAM.

## Objective
Provision and health-check all six services (Kafka KRaft, MinIO, Cassandra, Flink, Spark, Grafana)
via Docker Compose so that every component is reachable from the host and from other containers.

## Acceptance Criteria
- [ ] `docker compose up -d` starts all services with no exit codes
- [ ] Kafka: `kafka-topics.sh --bootstrap-server localhost:9092 --list` returns without error
- [ ] MinIO: `mc ls local/` returns bucket listing (MinIO Client configured)
- [ ] Cassandra: `cqlsh localhost 9042` opens a CQL shell
- [ ] Flink: Job Manager UI accessible at `http://localhost:8081`
- [ ] Spark: Master UI accessible at `http://localhost:8080`
- [ ] Grafana: UI accessible at `http://localhost:3000` (admin/admin)
- [ ] Screenshot of all containers running committed to repo as `docs/stack-health.png`

## Technical Hints
- Use **Kafka in KRaft mode** (no Zookeeper). Set `KAFKA_PROCESS_ROLES=broker,controller` and
  `KAFKA_NODE_ID=1`. Remove any Zookeeper service block entirely.
- Both Flink and Spark require the **S3A connector** to talk to MinIO. Mount the `hadoop-aws` JAR
  into their containers or add it to the image via a custom Dockerfile layer.
  Set env vars: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL=http://minio:9000`.
- Cassandra: expose CQL port `9042`. Set `MAX_HEAP_SIZE=512M` and `HEAP_NEWSIZE=128M` for a
  dev workstation. Use the `cassandra:4.1` image.
- Grafana: pre-install the `HadesArchitect-Cassandra-datasource` plugin by setting
  `GF_INSTALL_PLUGINS=hadesarchitect-cassandra-datasource` in environment.
- Flink: `1 JobManager + 1 TaskManager` is sufficient. Set `taskmanager.numberOfTaskSlots=4`.
- Use named Docker networks so services resolve each other by hostname (e.g. `kafka`, `minio`,
  `cassandra`).
- Reference: project brief §9.1 Infrastructure Setup.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
