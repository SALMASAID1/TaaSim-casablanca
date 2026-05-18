# Archivage Kafka Connect → MinIO (Zone Raw)

Ce projet réplique en direct les topics Kafka bruts dans MinIO sous le préfixe `raw/kafka-archive/`.

## Ce que vous obtenez

- Le topic `raw.gps` archivé dans `s3://taasim/raw/kafka-archive/raw.gps/...`
- Le topic `raw.trips` archivé dans `s3://taasim/raw/kafka-archive/raw.trips/...`

## Comment ça fonctionne

- Un exécuteur Kafka Connect (worker) s'exécute dans Docker (`kafka-connect`).
- Deux connecteurs S3 Sink sont enregistrés au démarrage (`kafka-connect-init`).
- Les configurations des connecteurs se trouvent dans :
  - `infra/kafka-connect/connectors/s3-sink-raw-gps.json`
  - `infra/kafka-connect/connectors/s3-sink-raw-trips.json`

Les identifiants et les points de terminaison (endpoints) sont résolus à partir des variables d'environnement en utilisant les fournisseurs de configuration de Kafka Connect (évitant ainsi d'écrire en dur les secrets dans le JSON) :

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_ENDPOINT_URL` (à l'intérieur de Docker, valeur par défaut `http://minio:9000` via compose)
- `AWS_REGION`

## Lancement des composants

Démarrez la stack :

- `docker compose up -d kafka minio minio-init kafka-connect kafka-connect-init`

Exécutez les deux producteurs (depuis la machine hôte) pour générer des données à archiver :

- `python -m src.producers.vehicle_gps_producer`
- `python -m src.producers.trip_request_producer`

## Vérification

Via l'API REST de Kafka Connect (port hôte 8084) :

- `curl -s http://localhost:8084/connectors | jq` (facultatif : `jq`)

Lister les objets archivés dans MinIO (en réutilisant l'image du service `minio-init` existante afin qu'elle rejoigne automatiquement le réseau Docker Compose) :

- `docker compose run --rm --no-deps --entrypoint sh minio-init -c 'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && mc ls "local/$MINIO_BUCKET/raw/kafka-archive/" --recursive | head'`

Vous devriez voir apparaître de nouveaux objets dans un délai d'environ ~2 minutes après le lancement des producteurs.

---

## Preuves d'exécution (capturées)

Capturées le **19-04-2026**.

### Connect — Connecteurs enregistrés

Commande :

```bash
curl -s -w "\nHTTP %{http_code}\n" http://localhost:8084/connectors
```

Sortie :

```text
["s3-sink-raw-trips","s3-sink-raw-gps"]
HTTP 200
```

### Connect — Statut des connecteurs

Commandes :

```bash
curl -s -w "\nHTTP %{http_code}\n" http://localhost:8084/connectors/s3-sink-raw-gps/status
curl -s -w "\nHTTP %{http_code}\n" http://localhost:8084/connectors/s3-sink-raw-trips/status
```

Sortie :

```text
{"name":"s3-sink-raw-gps","connector":{"state":"RUNNING","worker_id":"kafka-connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"kafka-connect:8083"}],"type":"sink"}
HTTP 200
{"name":"s3-sink-raw-trips","connector":{"state":"RUNNING","worker_id":"kafka-connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"kafka-connect:8083"}],"type":"sink"}
HTTP 200
```

### MinIO — Objets archivés

Commande :

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c 'mc alias set local "$MINIO_HOST" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls "local/$MINIO_BUCKET/raw/kafka-archive/" --recursive | head -n 20'
```

Sortie :

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
