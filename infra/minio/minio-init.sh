#!/bin/sh
set -eu

MINIO_HOST="${MINIO_HOST:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-taasim}"

mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing "local/$MINIO_BUCKET"

# Bucket layout: one bucket with standard prefixes
echo 'ok' | mc pipe "local/$MINIO_BUCKET/raw/porto-trips/.keep"
echo 'ok' | mc pipe "local/$MINIO_BUCKET/raw/nyc-tlc/.keep"
echo 'ok' | mc pipe "local/$MINIO_BUCKET/raw/kafka-archive/.keep"
echo 'ok' | mc pipe "local/$MINIO_BUCKET/raw/kafka-archive/flink-checkpoints/.keep"
echo 'ok' | mc pipe "local/$MINIO_BUCKET/raw/kafka-archive/flink-savepoints/.keep"

echo 'ok' | mc pipe "local/$MINIO_BUCKET/curated/trips/.keep"
echo 'ok' | mc pipe "local/$MINIO_BUCKET/curated/demand-by-zone/.keep"

echo 'ok' | mc pipe "local/$MINIO_BUCKET/ml/features/.keep"
echo 'ok' | mc pipe "local/$MINIO_BUCKET/ml/models/demand_v1/.keep"

echo 'ok' | mc pipe "local/$MINIO_BUCKET/metadata/.keep"

# -------------------------------------------------------------------
# Optional: seed MinIO from local workspace datasets (mounted read-only)
# This avoids re-uploading after every `docker compose up`.
# It only copies if a representative file is missing in the bucket.
# -------------------------------------------------------------------
if [ -f /seed/raw/porto-trips/train.csv ]; then
  if ! mc stat "local/$MINIO_BUCKET/raw/porto-trips/train.csv" >/dev/null 2>&1; then
    echo '--- Seeding Porto dataset into MinIO (raw/porto-trips) ---'
    mc cp /seed/raw/porto-trips/train.csv "local/$MINIO_BUCKET/raw/porto-trips/train.csv"
    [ -f /seed/raw/porto-trips/test.csv ] && mc cp /seed/raw/porto-trips/test.csv "local/$MINIO_BUCKET/raw/porto-trips/test.csv" || true
    [ -f /seed/raw/porto-trips/sampleSubmission.csv ] && mc cp /seed/raw/porto-trips/sampleSubmission.csv "local/$MINIO_BUCKET/raw/porto-trips/sampleSubmission.csv" || true
    [ -f /seed/raw/porto-trips/evaluation_script.r ] && mc cp /seed/raw/porto-trips/evaluation_script.r "local/$MINIO_BUCKET/raw/porto-trips/evaluation_script.r" || true
    if [ -d /seed/raw/porto-trips/metaData_taxistandsID_name_GPSlocation.csv ]; then
      mc mirror /seed/raw/porto-trips/metaData_taxistandsID_name_GPSlocation.csv "local/$MINIO_BUCKET/raw/porto-trips/metaData_taxistandsID_name_GPSlocation.csv" || true
    fi
  else
    echo '--- Porto dataset already present in MinIO ---'
  fi
else
  echo '--- No local Porto dataset found at ./raw/porto-trips/train.csv; skipping seed ---'
fi

if [ -f /seed/raw/nyc-tlc/yellow_tripdata_2019-01.parquet ]; then
  if ! mc stat "local/$MINIO_BUCKET/raw/nyc-tlc/yellow_tripdata_2019-01.parquet" >/dev/null 2>&1; then
    echo '--- Seeding NYC TLC dataset into MinIO (raw/nyc-tlc) ---'
    for f in /seed/raw/nyc-tlc/*.parquet; do
      [ -f "$f" ] || continue
      base=$(basename "$f")
      mc cp "$f" "local/$MINIO_BUCKET/raw/nyc-tlc/$base" || true
    done
  else
    echo '--- NYC TLC dataset already present in MinIO ---'
  fi
else
  echo '--- No local NYC TLC dataset found at ./raw/nyc-tlc/*.parquet; skipping seed ---'
fi

if [ -f /seed/metadata/zone_mapping.csv ]; then
  if ! mc stat "local/$MINIO_BUCKET/metadata/zone_mapping.csv" >/dev/null 2>&1; then
    echo '--- Seeding zone mapping into MinIO (metadata/zone_mapping.csv) ---'
    mc cp /seed/metadata/zone_mapping.csv "local/$MINIO_BUCKET/metadata/zone_mapping.csv"
  else
    echo '--- zone_mapping.csv already present in MinIO ---'
  fi
else
  echo '--- No local zone_mapping.csv found at ./metadata/zone_mapping.csv; skipping seed ---'
fi

echo '--- MinIO bucket + prefixes ready ---'
mc ls "local/$MINIO_BUCKET"
