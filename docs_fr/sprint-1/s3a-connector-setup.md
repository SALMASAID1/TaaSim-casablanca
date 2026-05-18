# Configuration du Connecteur S3A (Spark + Flink → MinIO)

Ce projet utilise **MinIO** comme stockage d'objets compatible S3 (avec le bucket `taasim`).

**Spark** et **Flink** doivent tous deux être en mesure de lire et d'écrire en utilisant les URIs du protocole `s3a://taasim/...`. Cela nécessite :

- Les JARs Hadoop AWS appropriés
- Une configuration correcte du point de terminaison (endpoint) et du style d'accès par chemin d'accès (path-style access) pour MinIO
- Des identifiants fournis via les variables d'environnement (aucun secret stocké en dur)

## Versions des JARs (figées)

Téléchargés par le script `infra/s3a-jars/s3a-jars-init.sh` dans le volume Docker nommé `s3a_jars` :

- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.262.jar`

Flink utilise quant à lui son plugin intégré de système de fichiers :

- `flink-s3-fs-hadoop-1.18.1.jar` (activé dans l'image Flink officielle)

## Configuration Spark

Spark charge ses paramètres S3A depuis le fichier `conf/spark-defaults.conf` (monté dans les conteneurs Spark et Jupyter).

Configurations clés :

- `spark.jars=/opt/extra-jars/hadoop-aws-3.3.4.jar,/opt/extra-jars/aws-java-sdk-bundle-1.12.262.jar`
- `spark.hadoop.fs.s3a.endpoint=http://minio:9000`
- `spark.hadoop.fs.s3a.path.style.access=true`
- `spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem`
- `spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider`

Les identifiants proviennent des variables d'environnement définies dans le Docker Compose (voir `x-s3a-env` dans `docker-compose.yml`) :

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_ENDPOINT_URL_DOCKER=http://minio:9000`

## Configuration Flink

Flink active le plugin de système de fichiers S3 intégré et définit les propriétés de connexion MinIO via la variable d'environnement `FLINK_PROPERTIES`.

Détail d'importance :
- L'image Docker officielle de Flink requiert la clé `ENABLE_BUILT_IN_PLUGINS` (avec les tirets bas de part et d'autre de `IN`), et non `ENABLE_BUILTIN_PLUGINS`.

Dans le fichier `docker-compose.yml`, nous définissons :

- `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-1.18.1.jar`

Et nous configurons :

- `s3.endpoint=http://minio:9000`
- `s3.path.style.access=true`
- `s3.access-key` / `s3.secret-key`
- `s3.connection.ssl.enabled=false`

---

## Tests de fumée (Smoke Tests)

Démarrez la stack :

```bash
docker compose up -d
```

### Test d'écriture Flink → MinIO (FileSink)

Nous utilisons l'exemple intégré WordCount de Flink (qui écrit vers un puits de système de fichiers lorsque le paramètre `--output` est renseigné) :

```bash
docker compose exec -T flink-jobmanager \
  flink run -m flink-jobmanager:8081 \
  /opt/flink/examples/streaming/WordCount.jar \
  --input /opt/flink/README.txt \
  --output s3a://taasim/raw/test-flink-write/wordcount
```

Vérifier la création des objets :

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc ls "local/$MINIO_BUCKET/raw/test-flink-write/" --recursive | head'
```

### Test de lecture & écriture Spark → MinIO

La commande suivante exécute un mini-job PySpark à l'intérieur du conteneur `jupyter` :

```bash
docker compose exec -T \
  -e PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip \
  jupyter \
  python -c "from pyspark.sql import SparkSession; spark=(SparkSession.builder.master('local[*]').appName('taasim-s3a-task03').getOrCreate()); df=spark.read.option('header','true').csv('s3a://taasim/raw/porto-trips/train.csv'); df.printSchema(); out='s3a://taasim/curated/test-spark-write/porto_train_sample_parquet'; df.limit(1000).write.mode('overwrite').parquet(out); print('WROTE', out); spark.stop()"
```

Vérifier la création des objets :

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null \
   && mc ls "local/$MINIO_BUCKET/curated/test-spark-write/" --recursive | head'
```

---

## Preuves d'exécution (capturées)

Capturées le **19-04-2026**.

### Soumission réussie du Job Flink

```text
Job has been submitted with JobID 2bb6ea6d8552f81daba63b65d91d5d00
Program execution finished
Job with JobID 2bb6ea6d8552f81daba63b65d91d5d00 has finished.
Job Runtime: 2280 ms
```

### MinIO contient les données générées par Flink

```text
[2026-04-19 14:23:33 UTC] 2.0KiB STANDARD wordcount/2026-04-19--14/part-8d81f9a2-bcda-40a1-9ca9-ec22f1f7c22c-0
```

### Succès du schéma Spark et de l'écriture

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

### MinIO contient les données générées par Spark

```text
[2026-04-19 14:25:44 UTC]     0B STANDARD porto_train_sample_parquet/_SUCCESS
[2026-04-19 14:25:44 UTC] 505KiB STANDARD porto_train_sample_parquet/part-00000-1418b781-635d-4381-8e4c-c239083840be-c000.snappy.parquet
```
