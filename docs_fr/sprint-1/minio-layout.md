# Structure des Buckets MinIO (TaaSim Casablanca)

MinIO constitue le lac de données (data lake) compatible S3 du projet. Nous utilisons un bucket unique nommé `taasim` organisé selon une architecture simple de « zones » basée sur les préfixes S3.

## Nom du Bucket

- `taasim`

## Zones / Préfixes

- `raw/` — Données d'entrée immuables et archive brute des flux (Kafka Connect)
- `curated/` — Données nettoyées/agrégées produites (Parquet)
- `ml/` — Caractéristiques (features) + modèles entraînés
- `metadata/` — Données de référence statiques (ex. cartographie des zones)

### Arborescence des préfixes S3

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

Notes :
- S3/MinIO ne disposant pas de dossiers physiques réels, ces préfixes sont créés par le dépôt de petits objets `.keep` vides.
- `raw/kafka-archive/` est alimenté par les connecteurs S3 Sink de Kafka Connect.

## Comment cette arborescence est créée

- Docker Compose lance un conteneur d'initialisation unique `minio-init` (basé sur l'image `minio/mc`).
- Le script d'initialisation [infra/minio/minio-init.sh](../infra/minio/minio-init.sh) crée le bucket, définit les préfixes et y injecte les données issues des dossiers locaux `raw/` et `metadata/` de l'espace de travail lorsqu'ils sont présents.

## Vérification (commandes)

### Liste des objets MinIO

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc ls "local/$MINIO_BUCKET" --recursive | head -n 40'
```

### Vérification de l'existence des objets clés

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc stat "local/$MINIO_BUCKET/raw/porto-trips/train.csv" \
   && mc stat "local/$MINIO_BUCKET/raw/nyc-tlc/yellow_tripdata_2019-01.parquet" \
   && mc stat "local/$MINIO_BUCKET/metadata/zone_mapping.csv"'
```

### Test de fumée PySpark S3A (dans le conteneur)

Le conteneur `jupyter/spark` contient les sources de PySpark sous `/usr/local/spark/python`, mais `PYTHONPATH` n'est pas configuré par défaut lors de l'utilisation de `docker compose exec`. Cette commande définit la variable d'environnement et vérifie la bonne lecture depuis MinIO via le protocole S3A :

```bash
docker compose exec -T \
  -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip \
  jupyter \
  python -c "from pyspark.sql import SparkSession; spark=SparkSession.builder.master('local[*]').appName('taasim-s3a-smoke').getOrCreate(); df=spark.read.option('header','true').csv('s3a://taasim/raw/porto-trips/train.csv'); print('ok columns', len(df.columns), df.columns[:6]); print('sample', df.limit(1).toJSON().collect()); spark.stop()"
```

---

## Preuves d'exécution (capturées)

Capturées le **19-04-2026**.

### MinIO — Liste récursive des préfixes (début)

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

### MinIO — Métriques des objets clés

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

### PySpark — Lecture S3A réussie

```text
ok columns 9 ['TRIP_ID', 'CALL_TYPE', 'ORIGIN_CALL', 'ORIGIN_STAND', 'TAXI_ID', 'TIMESTAMP']
sample ['{"TRIP_ID":"1372636858620000589",...}']
```
