# task07 — Kafka Connect S3 Archive (Kafka → MinIO `raw/kafka-archive/`)

## Context
The official course brief requires a durable **raw-zone archive** in MinIO under
`raw/kafka-archive/`. This is the “system of record” side of the Kappa architecture:
Kafka topics are mirrored into MinIO so downstream batch jobs (Spark ETL, ML feature
recomputation, replay/debug) do not depend on ephemeral consumer logs.

## Objective
Deploy a Kafka Connect worker and configure S3 Sink connectors to mirror at least
`raw.gps` and `raw.trips` into the MinIO bucket `taasim` under the prefix
`raw/kafka-archive/`.

## Acceptance Criteria
- [x] Kafka Connect worker runs in Docker (same Compose stack) and REST API is reachable
- [x] S3 Sink connector configured for topic `raw.gps`
- [x] S3 Sink connector configured for topic `raw.trips`
- [x] Both connectors write objects into MinIO bucket `taasim` under:
  - `raw/kafka-archive/raw.gps/…`
  - `raw/kafka-archive/raw.trips/…`
- [x] With both producers running, `mc ls local/taasim/raw/kafka-archive/ --recursive` shows
  new objects within 2 minutes
- [x] Connector configuration JSON files are committed under `infra/kafka-connect/connectors/`
- [x] A short setup note is documented in `docs/sprint-1/kafka-connect-s3-archive.md`

## Technical Hints
- Use a Kafka Connect image (e.g. `confluentinc/cp-kafka-connect`) with the S3 Sink plugin
  installed.
- Use MinIO S3 endpoint: `http://minio:9000` with path-style addressing.
- Typical connector config keys to look for:
  - `topics`
  - `s3.bucket.name=taasim`
  - `s3.region=us-east-1`
  - `store.url=http://minio:9000`
  - `s3.part.size`, `flush.size`
  - output format (JSON is fine for raw topics)
- Keep credentials and endpoints in environment variables and **do not hardcode secrets** in
  connector JSON.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
