# ADR-001 — Schéma Cassandra & clés de partitionnement

## Statut
Accepté — 19-04-2026

## Contexte
TaaSim a besoin d'un stockage de service à faible latence pour :

- Les positions en temps réel des véhicules (pour une carte/un tableau de bord)
- Le cycle de vie des trajets / l'historique des trajets
- Les agrégats de demande en temps réel par zone (carte thermique/heatmap)

Dans Cassandra, la **clé primaire représente le plan de requête** : les clés de partition doivent correspondre aux principaux modèles d'accès aux données.

## Décision
Nous créons l'espace de clés (keyspace) `taasim` et trois tables :

1) `vehicle_positions` avec pour clé primaire `((city, zone_id), event_time, taxi_id)` et ordre de clustering `event_time DESC, taxi_id ASC`.

- Pourquoi `(city, zone_id)` et non `taxi_id` ?
  - La requête attendue est « montre-moi les véhicules dans la zone X maintenant » (tableau de bord + API).
  - Le partitionnement par `taxi_id` transformerait cette requête en une opération de diffusion-collecte (scatter-gather) sur de nombreuses partitions.
  - Le clustering par `event_time DESC` fait de la lecture des « dernières positions » une lecture par tranches (slice read) ultra-rapide.
  - L'ajout de `taxi_id` à la clé de clustering empêche les écrasements lorsque les horodatages sont à la seconde près.
  - Un TTL (1 heure) empêche la table d'accumuler des positions obsolètes.

2) `trips` avec pour clé primaire `((city, date_bucket), created_at)` et ordre de clustering `created_at DESC`.

- Pourquoi `date_bucket` ?
  - L'historique des trajets est naturellement interrogé par plages horaires (ex. « aujourd'hui », « dernières 24h », « cette semaine »).
  - Une seule partition par ville grandirait indéfiniment et finirait par créer un point chaud (hot-spot).
  - Le découpage temporel par jour (bucketing) maintient les partitions délimitées et prévisibles.

3) `demand_zones` avec pour clé primaire `((city, zone_id), window_start)` et ordre de clustering `window_start DESC`.

- Pourquoi `(city, zone_id)` ?
  - Le modèle d'accès pour la carte thermique / les indicateurs clés (KPI) lit les « dernières fenêtres pour la zone X » (ou scanne les zones récentes).
  - Un TTL (24 heures) conserve uniquement les fenêtres récentes utilisées par le tableau de bord.

Le DDL idempotent est conservé dans `db/cassandra_init.cql` et appliqué par le job d'initialisation Docker Compose.

## Conséquences
- Lectures rapides pour les modèles de tableau de bord/API (lectures centrées sur les zones).
- Les composants d'écriture (Flink/FastAPI) doivent toujours fournir le paramètre `city` et les champs de découpage (bucketing) appropriés.
- La rétention historique est contrôlée via les TTL ; l'analyse à long terme appartient à MinIO (zones `raw/` / `curated/`).
