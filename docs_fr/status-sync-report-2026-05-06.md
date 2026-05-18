# TaaSim · Casablanca — Rapport de Synchronisation d'État (v2, basé sur les faits)

**Date du rapport :** 06-05-2026 03:53 (UTC+01:00)

**Portée :** Ce rapport est basé sur (a) l'inspection du dépôt/espace de travail et (b) des vérifications en direct de la stack s'exécutant actuellement sous Docker Compose.

---

## 0. Instantané des Preuves (Ce qui a été vérifié)

**Stack en direct (Docker Compose) :** 11 services sont actifs ; la plupart sont signalés comme sains (`healthy`).

**Flink (REST) :** Le Job **`job1-gps-normalizer`** est **RUNNING**.
- ID de Job : `68161f6bd7500b23b7091b446f47a8da`
- Points de contrôle (Checkpoints) : **55 terminés avec succès**, **0 échec**
- Chemin externe du point de contrôle : `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/68161f6bd7500b23b7091b446f47a8da/chk-55`

**Kafka (CLI du Broker) :**
- Les topics existent : `raw.gps`, `raw.trips`, `processed.gps`, `processed.demand`, `processed.matches` (+ internes de Connect)
- `raw.gps` : 4 partitions, rétention de 7 jours, offsets de fin **non nuls** (le topic contient des données)
- `raw.trips` : 4 partitions, offsets de fin à **0** (aucun événement de trajet observé dans le broker)
- `processed.gps` : offsets de fin **non nuls** (Le Job 1 produit des données sortantes)
- `processed.demand`, `processed.matches` : offsets de fin à **0** (Les Jobs 2/3 ne produisent pas encore)

**Kafka Connect (REST) :** Les connecteurs sont déployés et **RUNNING** :
- `s3-sink-raw-gps` (RUNNING)
- `s3-sink-raw-trips` (RUNNING)

**Cassandra (cqlsh) :** L'espace de clés (keyspace) `taasim` existe avec 3 tables :
- `vehicle_positions` (TTL 3600)
- `trips`
- `demand_zones` (TTL 86400)

**Données Cassandra :** `taasim.vehicle_positions` contient des lignes (Le Job 1 écrit activement).

**MinIO (vue système de fichiers conteneur) :** Le bucket `taasim` existe, et des préfixes existent pour :
- `raw/kafka-archive/` (incluant les checkpoints Flink)
- `curated/mapped_casa_trips/` (fichiers parquet enrichis présents)

**Grafana :** Le connecteur de source de données Cassandra est installé : `hadesarchitect-cassandra-datasource@3.2.0`.

---

## 1. Jalon Actuel (Correspondance avec le Plan)

Basé sur la cartographie du plan dans `plan/README.md` et le rapport d'état précédent daté du 03-05-2026 :
- Sprint 1 (Semaines 1–2) : **Fondations & Cartographie des Données** — ✅ Terminé (preuves archivées sous `docs_fr/sprint-1/`)
- Sprint 2 (Semaine 3) : **Normalisation GPS en Temps Réel** — ✅ Presque entièrement terminé (voir l'état du Sprint 2 ci-dessous)
- Sprint 3 (Semaine 4) : **Job 2/3 + Cartes Thermiques + Appariement** — 🔶 Prêt à démarrer

> Si votre calendrier de formation diffère, ajustez les libellés de semaine ; l'état *technique* ci-dessous est entièrement factuel.

---

## 2. État du Sprint

### Sprint 1 — Fondations & Cartographie des Données ✅ TERMINÉ (Preuves dans le Dépôt)

Preuves présentes dans `docs_fr/sprint-1/` :
- `stack-health.png`, `stack-health.txt`
- `casablanca-coordinate-validation.png`
- `kafka-connect-s3-archive.md`
- `minio-layout.md`
- `s3a-connector-setup.md`

### Sprint 2 — Normalisation GPS en Temps Réel ✅ LIVRABLES PRESQUE TOUS REMPLIS

**Confirmés faits (Dépôt + vérifications en direct) :**
- Job Flink 1 implémenté sous `flink_jobs/src/main/java/com/taasim/flink/job1/` (8 fichiers Java)
- JAR ombré (shaded) présent : `flink_jobs/target/taasim-flink-jobs-1.0.0-shaded.jar`
- Job Flink 1 en cours d'exécution (voir Instantané des Preuves) avec points de contrôle vers MinIO (`s3a://.../flink-checkpoints/...`)
- Les notes de tests de filigranes/checkpoints existent : `docs_fr/sprint-2/watermark-test-evidence.md`
- Les notes de vérification de l'anonymisation existent : `docs_fr/sprint-2/security-verification.md`
- Configuration automatique de Grafana présente :
  - Source de données : `grafana/provisioning/datasources/cassandra.yaml`
  - Configuration du tableau de bord : `grafana/provisioning/dashboards/default.yaml`
  - JSON du tableau de bord : `grafana/dashboards/taasim-live.json` (contient un panneau Geomap + des requêtes Cassandra)

**Non confirmés / Toujours en attente :**
- Le point de terminaison de FastAPI `/api/v1/vehicles/zone/{zone_id}` n'est **pas implémenté** (`src/api/` contient uniquement `.gitkeep`).
- Flux des trajets : `raw.trips` contient actuellement **0** message dans Kafka (le producteur de trajets n'a probablement pas tourné / n'a pas été sollicité).

---

## 3. Santé de l'Infrastructure (en Direct)

**Services actifs (docker compose ps) :**
- Kafka, Kafka UI, Kafka Connect
- MinIO
- Cassandra
- Flink JobManager + TaskManager
- Spark Master + Worker
- Jupyter
- Grafana

Tous sont actuellement en ligne et la plupart sont signalés comme sains (`healthy`).

---

## 4. Réalité des Flux de Données (ce qui circule en ce moment)

### 4.1 Flux GPS (✅ opérationnel)

Chaîne de bout en bout observée :
- Kafka `raw.gps` contient des données (offsets de fin d'environ ~1.7k–2.2k par partition au moment du rapport)
- Le Job Flink 1 est RUNNING et produit vers :
  - Cassandra `taasim.vehicle_positions` (lignes observées)
  - Kafka `processed.gps` (offsets de fin d'environ ~950–1245 par partition au moment du rapport)
- Le connecteur d'archivage S3 de Kafka Connect pour `raw.gps` est RUNNING et présente un faible décalage (quelques dizaines de messages) au moment du rapport

### 4.2 Flux des Trajets (⚠️ non actif)

- Les offsets de fin de Kafka `raw.trips` sont à **0** (aucun événement observé).
- Le connecteur d'archivage S3 pour `raw.trips` est RUNNING, mais il n'a encore rien à archiver.

---

## 5. Incohérences Détectées (éléments actuellement non alignés)

Ce sont des divergences observées entre le dépôt + stack en direct et le rapport d'état précédent `docs/status-sync-report.md` :

- **Uptime de la stack :** l'ancien rapport indiquait ~19h ; la stack actuelle montre des conteneurs créés il y a environ ~3h.
- **Offsets Kafka :** l'ancien rapport indiquait des offsets à 0 pour `raw.gps` ; le broker affiche actuellement des offsets **non nuls**.
- **Producteurs actifs :** l'ancien rapport indiquait que les producteurs GPS et trajets tournaient en même temps ; le broker indique que **le topic GPS est alimenté** mais **le topic trajets est vide**.
- **Grafana du Sprint 2 :** l'ancien rapport listait le panneau Geomap comme « non configuré » ; le dépôt contient pourtant la configuration, le plugin installé et le JSON du tableau de bord.
- **Listes de contrôle du plan :** plusieurs cases de tâches du plan « Critères d'acceptation » restent décochées alors que l'implémentation et les preuves existent (ex. Job 1, plugin Grafana).

---

## 6. Prochaines Étapes (Priorités)

**P0 (Terminer proprement le Sprint 2) :**
1. Implémenter le service FastAPI et le point de terminaison `/api/v1/vehicles/zone/{zone_id}`.
2. Lancer/valider le producteur de trajets pour alimenter `raw.trips` ; confirmer que le connecteur S3 écrit bien dans `raw/kafka-archive/raw.trips/`.
3. Ouvrir l'interface de Grafana et confirmer que les panneaux du tableau de bord s'affichent et s'actualisent correctement (mise à jour en temps réel des positions).

**P1 (Démarrer le Sprint 3) :**
4. Implémenter le Job Flink 2 (demand aggregation) écrivant dans `demand_zones` + `processed.demand`.
5. Implémenter le Job Flink 3 (trip matcher) écrivant dans `trips` + `processed.matches`.

**P2 (Hygiène documentaire) :**
6. Mettre à jour les cases des tâches du plan pour refléter la réalité et lier vers les fichiers de preuves.
7. Remplacer la narration de l'ancien rapport d'état par un modèle strictement basé sur les faits (ou conserver les deux comme v1/v2).
