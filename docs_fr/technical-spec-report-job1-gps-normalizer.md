# TaaSim Casablanca — Rapport de Spécifications Techniques du Job-1 Flink (GPS Normalizer)

Généré le : **05-05-2026**

Portée :
- Code inspecté : `flink_jobs/src/main/java/com/taasim/flink/job1/` (Pipeline du Job-1)
- Environnement en direct inspecté : Stack Docker Compose + API REST Flink sur `http://localhost:8081`

---

## 1) Architecture du Code

### 1.1 Arborescence des packages Java (`com.taasim.flink.job1`)

```text
flink_jobs/src/main/java/com/taasim/flink/job1/
├── Job1GpsNormalizer.java
├── functions/
│   ├── ParseGpsEventFn.java
│   ├── ValidationAndLateFilterFn.java
│   └── ZoneMappingBroadcastFn.java
├── model/
│   ├── GpsNormalizedEvent.java
│   ├── GpsRawEvent.java
│   └── ZoneDefinition.java
└── util/
    └── ZoneMappingLoader.java
```

### 1.2 Responsabilités (haut niveau)

- **`Job1GpsNormalizer`** : câble le pipeline de bout en bout (source Kafka → parsing → filigranes/watermarks → validation/filtrage des données tardives → cartographie des zones/anonymisation → puits Cassandra + Kafka), et configure les checkpoints ainsi que le RocksDB comme backend d'état.
- **`ParseGpsEventFn`** : analyse les chaînes de caractères JSON brutes pour les transformer en objets `GpsRawEvent`, en exposant des métriques pour les erreurs de parsing et les événements mal formés.
- **`ValidationAndLateFilterFn`** : supprime les événements non valides (en dehors de la boîte de délimitation de Casablanca ou vitesse aberrante) et élimine les événements tardifs en fonction du filigrane actuel ; les événements tardifs sont envoyés vers un flux secondaire (side output) nommé `late_events`.
- **`ZoneMappingBroadcastFn`** : joint les événements GPS à la cartographie des zones diffusée (broadcasted) ; réalise l'anonymisation en projetant (snapping) les coordonnées GPS brutes sur le centroïde de la zone correspondante.
- **`ZoneMappingLoader`** : charge le fichier `zone_mapping.csv` depuis le classpath (sourcé depuis `../metadata/zone_mapping.csv` via les ressources Maven).

---

## 2) Détail de la Logique

Cette section est basée sur le code source de :
- `Job1GpsNormalizer.java`
- `ValidationAndLateFilterFn.java`
- ainsi que sur l'implémentation de l'anonymisation dans `ZoneMappingBroadcastFn.java` (appelée par `Job1GpsNormalizer`).

### 2.1 Stratégie de filigrane + retard autorisé (exact)

**Stratégie de filigrane (comportement exact du code)**
- Les filigranes (watermarks) sont activés sur le flux de données `GpsRawEvent` analysé via :
  - `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(3))`
  - `withTimestampAssigner((event, ts) -> event.eventTimeMillis)`
- Intervalle d'émission automatique des filigranes :
  - `env.getConfig().setAutoWatermarkInterval(1_000L)` (1 seconde)

**Ce que signifie le « retard autorisé » (allowed lateness) pour ce Job**
- Il n'y a **aucun opérateur de fenêtre** avec `.allowedLateness(...)` dans le Job-1.
- Au lieu de cela, le retard est contrôlé explicitement dans `ValidationAndLateFilterFn` à l'aide du filigrane actuel :
  - `isLate(eventTimeMillis, currentWatermark)` renvoie `true` lorsque :
    - `currentWatermark != Long.MIN_VALUE` et `eventTimeMillis < currentWatermark`

**Seuil d'exclusion effective pour retard**
- La tolérance temporelle du Job est régie par le générateur de filigrane :
  - **Désalignement maximal (Bounded out-of-orderness) = 3 minutes**
  - Tout événement dont l'horodatage `eventTimeMillis` est antérieur au **filigrane actuel** est considéré comme **tardif** et est **exclu du flux principal**.

**Gestion des événements tardifs**
- Les événements tardifs sont envoyés vers une balise de sortie secondaire (side output tag) :
  - `ValidationAndLateFilterFn.LATE_EVENTS_TAG` (`"late_events"`)
- Dans `Job1GpsNormalizer`, cette sortie secondaire n'est **pas consommée** (les événements tardifs sont donc effectivement éliminés à moins qu'un autre opérateur ne les consomme ultérieurement).

### 2.2 Boîte de délimitation de validation de Casablanca (exact)

`ValidationAndLateFilterFn` applique une boîte de délimitation (bounding box) codée en dur pour Casablanca :

| Champ | Valeur |
|---|---:|
| `CASABLANCA_LON_MIN` | `-7.8` |
| `CASABLANCA_LON_MAX` | `-7.4` |
| `CASABLANCA_LAT_MIN` | `33.4` |
| `CASABLANCA_LAT_MAX` | `33.7` |

La logique de validation (`isInCasablancaBbox`) utilise des vérifications inclusives :
- `lon >= -7.8 && lon <= -7.4 && lat >= 33.4 && lat <= 33.7`

Validation additionnelle dans la même fonction :
- Vitesse maximale autorisée :
  - `MAX_SPEED_KMH = 150.0f`
  - `isSpeedValid(speedKmh)` renvoie true si et seulement si `speedKmh <= 150.0`

Métriques émises par cet opérateur :
- `invalid_bbox` (rejeté pour boîte de délimitation hors limites)
- `speed_too_high` (rejeté pour vitesse trop élevée)
- `dropped_late` (événements éliminés pour retard)

### 2.3 Anonymisation : projection des coordonnées sur les centroïdes de zone (exact)

**Où cela se produit**
- Dans `ZoneMappingBroadcastFn.processElement(...)`.

**Comment les zones sont définies**
- Les zones sont chargées à partir du fichier `zone_mapping.csv` via `ZoneMappingLoader.loadZonesFromClasspath()`.
- Chaque objet `ZoneDefinition` contient une boîte de délimitation rectangulaire :
  - `lonMin`, `lonMax`, `latMin`, `latMax`
- Un point est considéré à l'intérieur d'une zone en utilisant des bornes inclusives :
  - `ZoneDefinition.contains(lon, lat)` → `lon >= lonMin && lon <= lonMax && lat >= latMin && lat <= latMax`

**Comment les centroïdes sont calculés**
- `ZoneMappingLoader` calcule le centroïde de manière déterministe :
  - `centroidLon = (lonMin + lonMax) / 2.0`
  - `centroidLat = (latMin + latMax) / 2.0`

**Comportement d'appariement + projection de zone (exact)**
- Pour chaque événement GPS, la fonction parcourt les zones diffusées et sélectionne la **première** zone pour laquelle `zone.contains(value.lon, value.lat)` est vrai.
- Si une correspondance est trouvée, l'événement de sortie `GpsNormalizedEvent` est produit avec :
  - `normalized.zoneId = matched.arrondissementId`
  - `normalized.lat = matched.centroidLat`
  - `normalized.lon = matched.centroidLon`

Il s'agit de l'étape d'anonymisation : le couple `(lat, lon)` d'origine est remplacé par le **centroïde de la zone**.

---

## 3) Connectivité (Kafka + Cassandra)

### 3.1 Source Kafka (consommée)

Configurée dans `Job1GpsNormalizer` :
- Serveurs d'amorçage (paramètre, défaut) :
  - `--kafka-bootstrap-servers`, défaut `kafka:29092`
- Topic source (paramètre, défaut) :
  - `--source-topic`, défaut `raw.gps`
- ID du groupe de consommateurs (Consumer Group) :
  - `flink-job1-gps`
- Offsets de démarrage :
  - `OffsetsInitializer.earliest()`
- Désérialisation :
  - `SimpleStringSchema()` (consomme le JSON sous forme de chaînes de caractères brutes)

### 3.2 Puits Kafka (produit)

Configuré dans `Job1GpsNormalizer` :
- Topic de destination (paramètre, défaut) :
  - `--sink-topic`, défaut `processed.gps`
- Sémantique de livraison :
  - `DeliveryGuarantee.AT_LEAST_ONCE`
- Sérialisation des enregistrements :
  - Clé : `taxiId` (octets UTF-8)
  - Valeur : `GpsNormalizedEvent.toJson()` (octets UTF-8)

### 3.3 Puits Cassandra (table mise à jour)

`Job1GpsNormalizer` écrit dans Cassandra en utilisant exactement cette requête CQL :

```sql
INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);
```

Paramètres de connexion à l'exécution :
- `--cassandra-host` (défaut `cassandra`)
- `--cassandra-port` (défaut `9042`)

Confirmation du schéma :
- `db/cassandra_init.cql` définit `taasim.vehicle_positions` avec :
  - Clé primaire `((city, zone_id), event_time, taxi_id)`
  - Ordre de clustering `event_time DESC`
  - TTL `default_time_to_live = 3600`

---

## 4) État Opérationnel (Environnement Live Flink)

### 4.1 Moteur d'exécution Flink (Docker Compose)

Services observés en cours d'exécution (via `docker compose ps`) :
- Conteneur Flink JobManager : `taasim-flink-jm` (port `8081:8081`, sain)
- Conteneur Flink TaskManager : `taasim-flink-tm` (sain)

Point de terminaison REST Flink :
- `http://localhost:8081`

Image/version Flink (depuis le Docker Compose) :
- `flink:1.18.1-scala_2.12-java17`

### 4.2 Statut du Job pour l'ID `7e91ba535fc23e4e22fd89896ab1ab21`

**Résultat :** L'API REST de Flink signale cet ID de Job comme **non trouvé** au moment de l'inspection.

Preuve (REST) :
- `GET /jobs/7e91ba535fc23e4e22fd89896ab1ab21` → `NotFoundException: Job ... not found`

Interprétation :
- Cela signifie généralement que le Job n'est **pas en cours d'exécution** sur le cluster et n'est **pas disponible dans l'historique conservé** du JobManager (par exemple, redémarré avec un nouvel ID de Job, ou historique archivé purgé).

### 4.3 Instance active actuelle du Job-1 (pour référence)

`GET /jobs/overview` montre une instance active en cours d'exécution :
- Nom : `job1-gps-normalizer`
- ID de Job (actuel) : `472a09ecf5218af051829d14a67c3a21`
- État : `RUNNING`
- Heure de démarrage (UTC) : `2026-05-04T15:41:32.213Z`
- Dernière modification (UTC) : `2026-05-04T19:00:24.438Z`

### 4.4 Preuves de checkpointing (dernier point de contrôle réussi + chemin MinIO)

#### a) Configuration des checkpoints (depuis le code)

`Job1GpsNormalizer` configure :
- Intervalle de checkpoint par défaut : `60_000` (60s)
- Mode : `AT_LEAST_ONCE`
- Emplacement : `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`
- Pause minimale entre les points de contrôle : `30_000` (30s)
- Nombre maximal de checkpoints simultanés : `1`
- Backend d'état : `EmbeddedRocksDBStateBackend`

#### b) Statistiques de checkpoint REST Flink (ID de Job actif actuel)

Depuis `GET /jobs/472a09ecf5218af051829d14a67c3a21/checkpoints` :
- Dernier checkpoint réussi : **ID 366**
- Déclenchement (UTC) : `2026-05-05T10:46:52.474Z`
- Validation (ack) (UTC) : `2026-05-05T10:46:52.498Z`
- Chemin externe (MinIO / S3A) :
  - `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/472a09ecf5218af051829d14a67c3a21/chk-366`

Il indique également que le Job a été restauré à partir de :
- Checkpoint ID 198
- Heure de restauration (UTC) : `2026-05-04T18:59:35.621Z`
- Chemin de restauration :
  - `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/472a09ecf5218af051829d14a67c3a21/chk-198`

#### c) Preuves de log (JobManager)

Les journaux récents du JobManager montrent la création réussie du checkpoint et les chemins de commit vers S3 :

```text
2026-05-05 10:46:52,521 INFO  ...CheckpointCoordinator - Completed checkpoint 366 for job 472a09ecf5218af051829d14a67c3a21 ...
2026-05-05 10:46:52,513 INFO  ...S3Committer - Committing raw/kafka-archive/flink-checkpoints/job1/472a09ecf5218af051829d14a67c3a21/chk-366/_metadata ...
```

---

## Annexe A — Structure des événements d'entrée / sortie (pour débogage)

### A.1 JSON attendu en entrée brute (`raw.gps`)

`ParseGpsEventFn` attend ces champs :
- `taxi_id` (chaîne de caractères, requis)
- `timestamp` (chaîne de caractères, requis ; doit s'analyser avec `Instant.parse`, c'est-à-dire au format ISO-8601)
- `lat` (nombre ou chaîne numérique, requis)
- `lon` (nombre ou chaîne numérique, requis)
- `speed` (nombre ou chaîne numérique, requis)
- `status` (chaîne de caractères, requis)
- `trip_id` (chaîne de caractères, optionnel ; valeur par défaut : chaîne vide)

### A.2 JSON produit en sortie normalisée (`processed.gps`)

`GpsNormalizedEvent.toJson()` produit :
- `taxi_id`
- `timestamp` (horodatage d'origine si disponible, sinon dérivé de `eventTimeMillis`)
- `lat` / `lon` (centroïde de la zone)
- `speed`
- `status`
- `trip_id`
- `arrondissement_id` (ID de la zone)
