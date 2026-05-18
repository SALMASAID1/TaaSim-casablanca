# Job 1 (GPS Normalizer) — Contrats Système (Étape 0)

Ce document constitue la **source unique de vérité** définissant les données lues, écrites et les garanties que le Job-1 doit apporter. Vous implémenterez le Job-1 en Java (Maven) conformément à ces contrats.

---

## 1) Contrat d'Entrée — Kafka `raw.gps`

**Topic** : `raw.gps`

**Clé** : `taxi_id` (chaîne de caractères)

**Valeur** : JSON (sans registre de schéma). Produit par le producteur GPS.

### Champs requis

| Champ | Type | Exemple | Notes |
|---|---|---|---|
| `taxi_id` | string | `"20000528"` | Utilisé pour le routage des clés Kafka et l'unicité dans Cassandra |
| `timestamp` | string (ISO‑8601 UTC) | `"2026-04-20T14:30:12Z"` | Doit être analysable via `Instant.parse()` |
| `lat` | number | `33.592312` | Coordonnée géographique brute (ne doit jamais être persistée) |
| `lon` | number | `-7.612903` | Coordonnée géographique brute (ne doit jamais être persistée) |
| `speed` | number | `42.5` | Vitesse en km/h |
| `status` | string | `"available"` | Statut du taxi (format libre) |
| `trip_id` | string | `"1372636858620000589"` | Identifiant de trajet (optionnel / chaîne vide autorisée) |

### Validations (doivent avoir lieu avant toute écriture / persistance)

1. **Boîte de délimitation de Casablanca (bbox)**
   - Longitude dans l'intervalle $[-7.8, -7.4]$
   - Latitude dans l'intervalle $[33.4, 33.7]$
   - Si en dehors : **rejeter** l'événement et incrémenter la métrique `invalid_bbox`.

2. **Cohérence de la vitesse**
   - Si `speed > 150` : **rejeter** l'événement et incrémenter la métrique `speed_too_high`.

---

## 2) Contrat de Cartographie des Zones — `metadata/zone_mapping.csv`

**Fichier** : [metadata/zone_mapping.csv](../../metadata/zone_mapping.csv)

**Colonnes** :
- `arrondissement_id` (int de 1 à 16)
- `zone_name` (chaîne de caractères)
- `lon_min`, `lon_max`, `lat_min`, `lat_max` (boîte de délimitation de la zone)

### Règle d'association (Matching)
Un point GPS appartient à une zone si :
- `lon_min <= lon <= lon_max` ET `lat_min <= lat <= lat_max`.

Si aucune zone ne correspond : **rejeter** l'événement et incrémenter la métrique `zone_not_found`.

### Règle d'Anonymisation (Centroïde)
Les coordonnées géographiques stockées et émises DOIVENT être anonymisées en utilisant le centroïde de la zone associée.

Le centroïde est calculé à partir de la boîte de délimitation (bbox) :
$$
centroid\_lat = \frac{lat_{min}+lat_{max}}{2},\quad centroid\_lon = \frac{lon_{min}+lon_{max}}{2}
$$

---

## 3) Contrat de Sortie — Kafka `processed.gps`

**Topic** : `processed.gps`

**Clé** : `taxi_id` (chaîne de caractères)

**Valeur** : JSON reprenant les champs principaux de `raw.gps`, enrichi de l'ID de zone.

### Champs requis

Tous les champs de `raw.gps`, plus :
- `arrondissement_id` (int de 1 à 16)

### Garantie de confidentialité requise
Les champs `lat/lon` dans le topic `processed.gps` DOIVENT correspondre aux coordonnées anonymisées du centroïde de la zone (et ne jamais contenir les coordonnées brutes d'origine).

---

## 4) Contrat du Puits de Service — Cassandra `taasim.vehicle_positions`

**Table** : `taasim.vehicle_positions`

**Clé Primaire** : `((city, zone_id), event_time, taxi_id)`
- `city` est une constante pour ce projet : `casablanca`
- `event_time` est dérivé du champ `timestamp`
- `taxi_id` empêche les écrasements lorsque plusieurs véhicules partagent le même horodatage à la seconde près.

**Ordre de clustering** : `event_time DESC, taxi_id ASC`

**TTL** : 3600 secondes (1 heure)

### Colonnes écrites par le Job-1
- `city` (text)
- `zone_id` (int)
- `event_time` (timestamp)
- `taxi_id` (text)
- `lat` (double) — anonymisé
- `lon` (double) — anonymisé
- `speed` (float)
- `status` (text)

### Garantie de confidentialité requise
Les coordonnées brutes `lat/lon` issues de `raw.gps` ne doivent **jamais** apparaître dans cette table.

---

## 5) Contrat de Fiabilité du Temps Événementiel (Event-Time)

### Filigranes (Watermarks)
- Stratégie : Désalignement maximal toléré (Bounded out-of-orderness)
- Retard maximal autorisé : **3 minutes**

### Règle d'exclusion des données tardives (« Too late » rule)
Si l'horodatage d'un événement est **antérieur au filigrane actuel**, il est considéré comme trop tardif :
- **Rejeter** l'événement
- Incrémenter la métrique `dropped_late`
- (Optionnel) Diriger vers une sortie secondaire (side-output) `late_events` pour archivage / audit.

### Points de Contrôle (Checkpointing)
- Mode de garantie : `AT_LEAST_ONCE`
- Intervalle : `60s`
- Emplacement de stockage : `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`
- Backend d'état (State backend) : RocksDB

---

## 6) Configuration d'Exécution (Arguments Java)

Il est recommandé d'implémenter ces arguments CLI via le `ParameterTool` de Flink :
- `--kafka-bootstrap-servers` (défaut : `kafka:29092`)
- `--source-topic` (défaut : `raw.gps`)
- `--sink-topic` (défaut : `processed.gps`)
- `--cassandra-host` (défaut : `cassandra`)
- `--cassandra-port` (défaut : `9042`)
- `--city` (défaut : `casablanca`)
- `--checkpoint-dir` (défaut : `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`)
- `--checkpoint-interval-ms` (défaut : `60000`)

---

## 7) Définition du Fini / Done (Sprint-2)

Le Job-1 est considéré comme « terminé » lorsque :
- Il s'exécute pendant 10 minutes sans lever d'exception.
- Il écrit des lignes anonymisées dans Cassandra.
- Il transfère les événements anonymisés vers le topic `processed.gps`.
- Les résultats des tests de filigranes (retard de 2 min accepté, retard de 4 min rejeté) sont documentés dans les preuves du Sprint-2.
- Un test automatisé confirme que les coordonnées brutes ne sont jamais écrites en base.
