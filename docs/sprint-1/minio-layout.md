# MinIO bucket layout (TaaSim Casablanca)

MinIO is the project’s S3-compatible data lake. We use a single bucket named `taasim` with a
simple prefix-based “zone” layout.

## Bucket name

- `taasim`

## Zones / prefixes

- `raw/` — immutable inputs and raw stream archive (Kafka Connect)
- `curated/` — cleaned/aggregated outputs (Parquet)
- `ml/` — features + trained models
- `metadata/` — static reference data (e.g., zone mapping)

### Prefix tree

```text
s3://taasim/
  raw/
    porto-trips/
      train.csv
      test.csv
      sampleSubmission.csv
      evaluation_script.r
      metaData_taxistandsID_name_GPSlocation.csv/...
    nyc-tlc/
      yellow_tripdata_2019-01.parquet
      yellow_tripdata_2019-02.parquet
      yellow_tripdata_2019-03.parquet
    kafka-archive/
      raw.gps/...
      raw.trips/...
      flink-checkpoints/...
      flink-savepoints/...

  curated/
    trips/...
    demand-by-zone/...

  ml/
    features/...
    models/
      demand_v1/...

  metadata/
    zone_mapping.csv
```

Notes:
- S3/MinIO has no real folders; prefixes are created by uploading small `.keep` objects.
- `raw/kafka-archive/` is filled by Kafka Connect S3 Sink connectors.

## How this is created

- Docker Compose runs a one-shot init container `minio-init` (image `minio/mc`).
- The init script [infra/minio/minio-init.sh](../infra/minio/minio-init.sh) creates the bucket +
  prefixes and seeds data from the local workspace `raw/` and `metadata/` folders when present.

## Verify (commands)

### MinIO listing

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc ls "local/$MINIO_BUCKET" --recursive | head -n 40'
```

### Key objects exist

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc stat "local/$MINIO_BUCKET/raw/porto-trips/train.csv" \
   && mc stat "local/$MINIO_BUCKET/raw/nyc-tlc/yellow_tripdata_2019-01.parquet" \
   && mc stat "local/$MINIO_BUCKET/metadata/zone_mapping.csv"'
```

### PySpark S3A smoke test (inside container)

The jupyter/spark image contains Spark’s `pyspark/` sources under `/usr/local/spark/python`, but
`PYTHONPATH` is not set by default when using `docker compose exec`. This command sets it and
verifies reading from MinIO with S3A:

```bash
docker compose exec -T \
  -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip \
  jupyter \
  python -c "from pyspark.sql import SparkSession; spark=SparkSession.builder.master('local[*]').appName('taasim-s3a-smoke').getOrCreate(); df=spark.read.option('header','true').csv('s3a://taasim/raw/porto-trips/train.csv'); print('ok columns', len(df.columns), df.columns[:6]); print('sample', df.limit(1).toJSON().collect()); spark.stop()"
```

## Evidence (captured)

Captured on 2026-04-19.

### MinIO — recursive prefix listing (head)

```text
[2026-04-19 12:51:01 UTC]     3B STANDARD curated/demand-by-zone/.keep
[2026-04-19 12:51:01 UTC]     3B STANDARD curated/trips/.keep
[2026-04-19 12:51:01 UTC]     3B STANDARD metadata/.keep
[2026-04-17 19:26:05 UTC]   716B STANDARD metadata/zone_mapping.csv
[2026-04-19 12:51:01 UTC]     3B STANDARD ml/features/.keep
[2026-04-19 12:51:01 UTC]     3B STANDARD ml/models/demand_v1/.keep
[2026-04-19 12:51:01 UTC]     3B STANDARD raw/kafka-archive/.keep
[2026-04-19 12:51:01 UTC]     3B STANDARD raw/kafka-archive/flink-checkpoints/.keep
[2026-04-19 12:51:01 UTC]     3B STANDARD raw/kafka-archive/flink-savepoints/.keep
[2026-04-19 12:51:34 UTC] 1.7KiB STANDARD raw/kafka-archive/raw.gps/partition=2/raw.gps+2+0000000000.json.gz
[2026-04-19 12:51:34 UTC] 1.8KiB STANDARD raw/kafka-archive/raw.gps/partition=3/raw.gps+3+0000000000.json.gz
[2026-04-19 12:51:34 UTC] 4.1KiB STANDARD raw/kafka-archive/raw.trips/partition=0/raw.trips+0+0000000000.json.gz
...
```

### MinIO — object stats

```text
Name      : train.csv
Date      : 2026-04-17 19:26:04 UTC
Size      : 1.8 GiB
Type      : file

Name      : yellow_tripdata_2019-01.parquet
Date      : 2026-04-17 19:26:04 UTC
Size      : 105 MiB
Type      : file

Name      : zone_mapping.csv
Date      : 2026-04-17 19:26:05 UTC
Size      : 716 B
Type      : file
```

### PySpark — S3A read succeeded

```text
ok columns 9 ['TRIP_ID', 'CALL_TYPE', 'ORIGIN_CALL', 'ORIGIN_STAND', 'TAXI_ID', 'TIMESTAMP']
sample ['{"TRIP_ID":"1372636858620000589",...}']
```
