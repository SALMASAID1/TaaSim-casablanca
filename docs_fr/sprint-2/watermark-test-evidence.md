# Preuves d'Exécution des Filigranes (Watermarks) & Points de Contrôle (Checkpoints) (Sprint 2)

## Objectif
Démontrer le bon fonctionnement du **traitement basé sur le temps événementiel (event-time)** avec un **retard maximal toléré de 3 minutes pour le filigrane** et le bon fonctionnement des **points de contrôle Flink vers MinIO**.

## Configuration
- Job Flink 1 déployé et actif
- Intervalle de point de contrôle (checkpoint) : 60s
- Stockage des checkpoints : MinIO (via S3A)
- Répertoire des checkpoints : `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`

## Cas de Test

### Cas A — Retard de 2 minutes (doit être traité)
- Injecter un événement GPS avec un horodatage `timestamp = maintenant - 2 minutes`
- Attendu : l'événement est traité et enregistré dans Cassandra.

### Cas B — Retard de 4 minutes (doit être rejeté)
- Injecter un événement GPS avec un horodatage `timestamp = maintenant - 4 minutes`
- Attendu : l'événement est rejeté et le compteur d'événements tardifs (`dropped_late`) est incrémenté.

---

## Preuves d'exécution

### Exécution (21-04-2026) — Deuxième essai

#### Job Flink
- Nom du Job : `job1-gps-normalizer`
- ID du Job (RUNNING) : `36f54e632ce9db664d2ea9e266492573`

#### Identifiants de Test
- run_id : `20260421T181658Z`
- taxi_id (clé) : `wm_tryagain_20260421T181658Z`

#### Notes sur les partitions
Le topic `raw.gps` dispose de **4 partitions**. Les filigranes sont calculés par partition, et le filigrane global effectif correspond au **minimum** de l'ensemble des partitions.

Afin de rendre le test de retard déterministe, nous avons d'abord fait avancer le temps événementiel sur **toutes les partitions** en publiant un enregistrement de référence (« base timestamp ») sur chacune d'elles. Pour les partitions n'intervenant pas directement dans le test, nous avons renseigné une vitesse fictive `speed=200` afin que l'événement soit éliminé par les filtres de validation (donc non persisté dans Cassandra) tout en faisant avancer efficacement le filigrane (l'attribution du timestamp ayant lieu avant les filtres de validation).

#### Commandes de reproduction (repro)

```bash
# Téléchargement du JAR (après redémarrage à blanc de Flink)
curl -sf -X POST -H 'Expect:' -F 'jarfile=@flink_jobs/target/taasim-flink-jobs-1.0.0-shaded.jar' \
	http://localhost:8081/jars/upload

# Exécution du Job-1
curl -sS -X POST -H 'Content-Type: application/json' \
	-d '{"entryClass":"com.taasim.flink.job1.Job1GpsNormalizer"}' \
	http://localhost:8081/jars/79dc5214-ea24-40ae-8ed9-d75517144468_taasim-flink-jobs-1.0.0-shaded.jar/run

# Description du nombre de partitions Kafka
docker exec -i taasim-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic raw.gps

# Horodatages utilisés (dérivés du filigrane effectif au début du test)
# TS_BASE=2026-04-21T17:41:59Z
# TS_L2  =2026-04-21T17:39:59Z
# TS_L4  =2026-04-21T17:37:59Z

# Enregistrement BASE pour la clé de test (partition 1)
python3 -c 'import json; key="wm_tryagain_20260421T181658Z"; ts="2026-04-21T17:41:59Z"; evt={"taxi_id":key,"trip_id":"base","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":30.0,"status":"available"}; print(key+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Avancement des autres partitions (clés associées aux partitions 0/2/3) — vitesse non valide pour bloquer la persistance
python3 -c 'import json; ts="2026-04-21T17:41:59Z"; 
keys=["wm_adv_2","wm_adv_3","wm_adv_0"]; 
for k in keys:
  evt={"taxi_id":k,"trip_id":"adv","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":200.0,"status":"available"};
  print(k+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Cas A : retard de 2 minutes (doit être traité)
python3 -c 'import json; key="wm_tryagain_20260421T181658Z"; ts="2026-04-21T17:39:59Z"; evt={"taxi_id":key,"trip_id":"late2m","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":30.0,"status":"available"}; print(key+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Cas B : retard de 4 minutes (doit être rejeté)
python3 -c 'import json; key="wm_tryagain_20260421T181658Z"; ts="2026-04-21T17:37:59Z"; evt={"taxi_id":key,"trip_id":"late4m","timestamp":ts,"lat":33.605,"lon":-7.61,"speed":30.0,"status":"available"}; print(key+"|"+json.dumps(evt,separators=(",",":")))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic raw.gps --property parse.key=true --property key.separator='|'

# Récupération de la métrique d'exclusion
curl -sS \
	"http://localhost:8081/jobs/36f54e632ce9db664d2ea9e266492573/vertices/cbc357ccb763df2852fee8c4fc7d55f2/metrics?get=0.validate-and-late-filter.dropped_late"
```

#### Cassandra (Cas A persisté, Cas B absent)

```sql
SELECT event_time, taxi_id, speed, status
FROM taasim.vehicle_positions
WHERE city='casablanca' AND zone_id=1 AND taxi_id='wm_tryagain_20260421T181658Z'
ALLOW FILTERING;
```

```text
 event_time                      | taxi_id                      | speed | status
---------------------------------+------------------------------+-------+-----------
 2026-04-21 17:41:59.000000+0000 | wm_tryagain_20260421T181658Z |    30 | available
 2026-04-21 17:39:59.000000+0000 | wm_tryagain_20260421T181658Z |    30 | available

(2 rows)
```

L'enregistrement accusant 4 minutes de retard à `2026-04-21T17:37:59Z` n'apparaît pas dans Cassandra.

#### Métrique Flink (compteur dropped_late incrémenté pour le Cas B)

```json
[{"id":"0.validate-and-late-filter.dropped_late","value":"1"}]
```

#### Points de contrôle (Statistiques Flink REST + MinIO)

Résumé des points de contrôle issus de Flink REST :

```text
counts= {'restored': 0, 'total': 34, 'in_progress': 0, 'completed': 34, 'failed': 0}
latest_completed_external_path= s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/36f54e632ce9db664d2ea9e266492573/chk-34
latest_completed_status= COMPLETED
```

Contenu du répertoire sous MinIO (vue du conteneur local) :

```text
/data/taasim/raw/kafka-archive/flink-checkpoints/job1/36f54e632ce9db664d2ea9e266492573:
chk-34
shared__XLDIR__
taskowned__XLDIR__
```

---

### Exécution (20-04-2026)

#### Job Flink
- Nom du Job : `job1-gps-normalizer`
- ID du Job (RUNNING) : `fab713a45dc5ddd3c1ffb4522375ca9c`

#### Commandes de reproduction (repro)

```bash
# Compilation du JAR ombré
cd flink_jobs
mvn -q -DskipTests package

# Téléchargement & lancement sur Flink
curl -sf -X POST -H 'Expect:' -F 'jarfile=@flink_jobs/target/taasim-flink-jobs-1.0.0-shaded.jar' \
	http://localhost:8081/jars/upload

curl -sf -X POST \
	"http://localhost:8081/jars/<jar-id>/run?entry-class=com.taasim.flink.job1.Job1GpsNormalizer"

# Injection des événements de référence et en retard
python3 -c 'import json,datetime; now=datetime.datetime.now(datetime.timezone.utc); msg={"taxi_id":"wm_base_001","timestamp":now.isoformat().replace("+00:00","Z"),"lat":33.600,"lon":-7.610,"speed":35.0,"status":"AVAILABLE","trip_id":"trip_test_base"}; print(json.dumps(msg))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic raw.gps

python3 -c 'import json,datetime; now=datetime.datetime.now(datetime.timezone.utc); base={"lat":33.600,"lon":-7.610,"speed":35.0,"status":"AVAILABLE"};
for taxi_id,delta,trip in [("wm_late_2m_001",120,"trip_test_2m"),("wm_late_4m_001",240,"trip_test_4m")]:
	ts=(now-datetime.timedelta(seconds=delta)).isoformat().replace("+00:00","Z");
	msg={"taxi_id":taxi_id,"timestamp":ts,**base,"trip_id":trip};
	print(json.dumps(msg))' \
	| docker exec -i taasim-kafka kafka-console-producer --bootstrap-server kafka:29092 --topic raw.gps

# Récupération de la métrique
curl -s \
	"http://localhost:8081/jobs/fab713a45dc5ddd3c1ffb4522375ca9c/vertices/cbc357ccb763df2852fee8c4fc7d55f2/metrics?get=0.validate-and-late-filter.dropped_late"

# Consultation des objets MinIO
docker run --rm --network taasim-casablanca_taasim \
	-e MC_HOST_minio=http://minioadmin:minioadmin@minio:9000 \
	minio/mc ls minio/taasim/raw/kafka-archive/flink-checkpoints/job1/fab713a45dc5ddd3c1ffb4522375ca9c/
```

#### Kafka → `processed.gps` (démontre que la sortie du Cas A existe)

Messages observés dans `processed.gps` pour les identifiants de test :

```text
wm_base_001|{"taxi_id":"wm_base_001","timestamp":"2026-04-20T21:18:25.044713Z","lat":33.605000000000004,"lon":-7.609999999999999,"speed":35.0,"status":"AVAILABLE","trip_id":"trip_test_base","arrondissement_id":1}
wm_late_2m_001|{"taxi_id":"wm_late_2m_001","timestamp":"2026-04-20T21:17:08.247876Z","lat":33.605000000000004,"lon":-7.609999999999999,"speed":35.0,"status":"AVAILABLE","trip_id":"trip_test_2m","arrondissement_id":1}
```

#### Cassandra (Cas A persisté, Cas B absent)

Requête d'interrogation :

```sql
SELECT event_time, taxi_id, lat, lon, speed, status
FROM taasim.vehicle_positions
WHERE city='casablanca' AND zone_id=1
LIMIT 20;
```

Sortie obtenue (notez l'anonymisation par centroïde : coordonnées réelles d'entrée lat=33.600/lon=-7.610 → enregistrement persisté lat=33.605/lon=-7.610) :

```text
 event_time                      | taxi_id        | lat    | lon   | speed | status
---------------------------------+----------------+--------+-------+-------+-----------
 2026-04-20 21:18:25.044000+0000 |    wm_base_001 | 33.605 | -7.61 |    35 | AVAILABLE
 2026-04-20 21:17:08.247000+0000 | wm_late_2m_001 | 33.605 | -7.61 |    35 | AVAILABLE

(2 rows)
```

L'identifiant `wm_late_4m_001` n'apparaît pas dans la table Cassandra, ce qui confirme son rejet effectif en raison du dépassement de la tolérance temporelle.

#### Métrique Flink (compteur `dropped_late` incrémenté pour le Cas B)

```json
[{"id":"0.validate-and-late-filter.dropped_late","value":"1"}]
```

#### Points de contrôle (Statistiques Flink REST + MinIO)

Métrique de checkpoint Flink REST :

```text
counts= {'restored': 0, 'total': 30, 'in_progress': 0, 'completed': 30, 'failed': 0}
latest_completed= {... 'status': 'COMPLETED', ... 'external_path': 's3a://taasim/raw/kafka-archive/flink-checkpoints/job1/fab713a45dc5ddd3c1ffb4522375ca9c/chk-30', ...}
```

Arborescence MinIO (via `mc`) :

```text
s3://taasim/raw/kafka-archive/flink-checkpoints/job1/
	fab713a45dc5ddd3c1ffb4522375ca9c/
		chk-29/
		shared/
		taskowned/

s3://taasim/raw/kafka-archive/flink-checkpoints/job1/fab713a45dc5ddd3c1ffb4522375ca9c/chk-29/
	_metadata
```

## Liste de Contrôle de Validation
- [x] Liste d'arborescence MinIO prouvant la création du point de contrôle
- [x] Preuve du traitement effectif du Cas A (Cassandra + `processed.gps`)
- [x] Preuve du rejet effectif du Cas B (compteur `dropped_late=1` + absence Cassandra)
