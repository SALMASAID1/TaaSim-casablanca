# Kafka Connect → MinIO archive (raw zone)

This project mirrors Kafka raw topics into MinIO under `raw/kafka-archive/`.

## What you get

- Topic `raw.gps` archived to `s3://taasim/raw/kafka-archive/raw.gps/...`
- Topic `raw.trips` archived to `s3://taasim/raw/kafka-archive/raw.trips/...`

## How it works

- A Kafka Connect worker runs in Docker (`kafka-connect`).
- Two S3 Sink connectors are registered on startup (`kafka-connect-init`).
- Connector configs live in:
  - `infra/kafka-connect/connectors/s3-sink-raw-gps.json`
  - `infra/kafka-connect/connectors/s3-sink-raw-trips.json`

Credentials/endpoints are resolved from environment variables using Kafka Connect config providers
(so we don’t hardcode secrets in the JSON):

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_ENDPOINT_URL` (inside Docker defaults to `http://minio:9000` via compose)
- `AWS_REGION`

## Run

Start the stack:

- `docker compose up -d kafka minio minio-init kafka-connect kafka-connect-init`

Run both producers (from host) so there is data to archive:

- `python -m src.producers.vehicle_gps_producer`
- `python -m src.producers.trip_request_producer`

## Verify

Kafka Connect REST (host port 8084):

- `curl -s http://localhost:8084/connectors | jq` (optional: `jq`)

List archived objects in MinIO (using the existing `minio-init` service image, so it joins the Compose network automatically):

- `docker compose run --rm --no-deps --entrypoint sh minio-init -c 'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls "local/$MINIO_BUCKET/raw/kafka-archive/" --recursive | head'`

You should see new objects within ~2 minutes after starting the producers.

## Evidence (captured)

Captured on 2026-04-19.

### Connect — registered connectors

Command:

```bash
curl -s -w "\nHTTP %{http_code}\n" http://localhost:8084/connectors
```

Output:

```text
["s3-sink-raw-trips","s3-sink-raw-gps"]
HTTP 200
```

### Connect — connector status

Commands:

```bash
curl -s -w "\nHTTP %{http_code}\n" http://localhost:8084/connectors/s3-sink-raw-gps/status
curl -s -w "\nHTTP %{http_code}\n" http://localhost:8084/connectors/s3-sink-raw-trips/status
```

Output:

```text
{"name":"s3-sink-raw-gps","connector":{"state":"RUNNING","worker_id":"kafka-connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"kafka-connect:8083"}],"type":"sink"}
HTTP 200
{"name":"s3-sink-raw-trips","connector":{"state":"RUNNING","worker_id":"kafka-connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"kafka-connect:8083"}],"type":"sink"}
HTTP 200
```

### MinIO — archived objects

Command:

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c 'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls "local/$MINIO_BUCKET/raw/kafka-archive/" --recursive | head -n 20'
```

Output:

```text
[2026-04-19 12:51:01 UTC]     3B STANDARD .keep
[2026-04-19 12:51:01 UTC]     3B STANDARD flink-checkpoints/.keep
[2026-04-19 12:51:01 UTC]     3B STANDARD flink-savepoints/.keep
[2026-04-19 12:51:34 UTC] 1.7KiB STANDARD raw.gps/partition=2/raw.gps+2+0000000000.json.gz
[2026-04-19 12:51:34 UTC] 1.8KiB STANDARD raw.gps/partition=3/raw.gps+3+0000000000.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=0/raw.trips+0+0000000000.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=0/raw.trips+0+0000000100.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=0/raw.trips+0+0000000200.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=0/raw.trips+0+0000000300.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=0/raw.trips+0+0000000400.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=0/raw.trips+0+0000000500.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=0/raw.trips+0+0000000600.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=1/raw.trips+1+0000000000.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=1/raw.trips+1+0000000100.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=1/raw.trips+1+0000000200.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=1/raw.trips+1+0000000300.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=1/raw.trips+1+0000000400.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=1/raw.trips+1+0000000500.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=1/raw.trips+1+0000000600.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw.trips/partition=2/raw.trips+2+0000000000.json.gz
```
