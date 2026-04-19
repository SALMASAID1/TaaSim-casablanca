# S3A connector setup (Spark + Flink → MinIO)

This project uses **MinIO** as an S3-compatible object store (bucket `taasim`).

Both **Spark** and **Flink** must be able to read/write using `s3a://taasim/...` URIs.
This requires:

- Correct Hadoop AWS JARs
- Correct endpoint + path-style configuration for MinIO
- Credentials provided via environment variables (no secrets hard-coded)

## JAR versions (pinned)

Downloaded by `infra/s3a-jars/s3a-jars-init.sh` into the named Docker volume `s3a_jars`:

- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.262.jar`

Flink uses the built-in filesystem plugin:

- `flink-s3-fs-hadoop-1.18.1.jar` (enabled in the official Flink image)

## Spark configuration

Spark reads its S3A settings from `conf/spark-defaults.conf` (mounted into the Spark + Jupyter containers).

Key settings:

- `spark.jars=/opt/extra-jars/hadoop-aws-3.3.4.jar,/opt/extra-jars/aws-java-sdk-bundle-1.12.262.jar`
- `spark.hadoop.fs.s3a.endpoint=http://minio:9000`
- `spark.hadoop.fs.s3a.path.style.access=true`
- `spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
- `spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider`

Credentials come from env vars in Compose (see `x-s3a-env` in `docker-compose.yml`):

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_ENDPOINT_URL_DOCKER=http://minio:9000`

## Flink configuration

Flink enables the built-in S3 filesystem plugin and sets MinIO connection properties via `FLINK_PROPERTIES`.

Important detail:

- The official Flink Docker image expects `ENABLE_BUILT_IN_PLUGINS` (with `_IN_`), not `ENABLE_BUILTIN_PLUGINS`.

In `docker-compose.yml` we set:

- `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.18.1.jar`

And we configure:

- `s3.endpoint=http://minio:9000`
- `s3.path.style.access=true`
- `s3.access-key` / `s3.secret-key`
- `s3.connection.ssl.enabled=false`

## Smoke tests

Start the stack:

```bash
docker compose up -d
```

### Flink → MinIO write test (FileSink)

We use Flink’s built-in WordCount example (it writes to a filesystem sink when `--output` is set):

```bash
docker compose exec -T flink-jobmanager \
  flink run -m flink-jobmanager:8081 \
  /opt/flink/examples/streaming/WordCount.jar \
  --input /opt/flink/README.txt \
  --output s3a://taasim/raw/test-flink-write/wordcount
```

Verify objects exist:

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc ls "local/$MINIO_BUCKET/raw/test-flink-write/" --recursive | head'
```

### Spark → MinIO read + write test

The following command runs a tiny PySpark job inside the `jupyter` container:

```bash
docker compose exec -T \
  -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip \
  jupyter \
  python -c "from pyspark.sql import SparkSession; spark=(SparkSession.builder.master('local[*]').appName('taasim-s3a-task03').getOrCreate()); df=spark.read.option('header','true').csv('s3a://taasim/raw/porto-trips/train.csv'); df.printSchema(); out='s3a://taasim/curated/test-spark-write/porto_train_sample_parquet'; df.limit(1000).write.mode('overwrite').parquet(out); print('WROTE', out); spark.stop()"
```

Verify objects exist:

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc ls "local/$MINIO_BUCKET/curated/test-spark-write/" --recursive | head'
```

## Evidence (captured)

Captured on 2026-04-19.

### Flink job succeeded

```text
Job has been submitted with JobID 2bb6ea6d8552f81daba63b65d91d5d00
Program execution finished
Job with JobID 2bb6ea6d8552f81daba63b65d91d5d00 has finished.
Job Runtime: 2280 ms
```

### MinIO contains Flink output

```text
[2026-04-19 14:23:33 UTC] 2.0KiB STANDARD wordcount/2026-04-19--14/part-8d81f9a2-bcda-40a1-9ca9-ec22f1f7c22c-0
```

### Spark schema + write succeeded

```text
root
 |-- TRIP_ID: string (nullable = true)
 |-- CALL_TYPE: string (nullable = true)
 |-- ORIGIN_CALL: string (nullable = true)
 |-- ORIGIN_STAND: string (nullable = true)
 |-- TAXI_ID: string (nullable = true)
 |-- TIMESTAMP: string (nullable = true)
 |-- DAY_TYPE: string (nullable = true)
 |-- MISSING_DATA: string (nullable = true)
 |-- POLYLINE: string (nullable = true)

WROTE s3a://taasim/curated/test-spark-write/porto_train_sample_parquet
```

### MinIO contains Spark output

```text
[2026-04-19 14:25:44 UTC]     0B STANDARD porto_train_sample_parquet/_SUCCESS
[2026-04-19 14:25:44 UTC] 505KiB STANDARD porto_train_sample_parquet/part-00000-1418b781-635d-4381-8e4c-c239083840be-c000.snappy.parquet
```
