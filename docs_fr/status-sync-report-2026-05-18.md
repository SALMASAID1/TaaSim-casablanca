# TaaSim · Casablanca — Rapport de Synchronisation d'État (Daily Progress Sync)

**Date du rapport :** 18-05-2026 · 17:55 (Afrique/Casablanca)  
**Calendrier actuel du Lab :** **Semaine 6 sur 8** (Démarre aujourd'hui, 18 mai 2026)  
**Jalon Chronologique Actuel :** **Sprint 5 — ML, Intelligence & Renforcement de la Sécurité**  
**État de l'Implémentation Technique :** **Sprint 3 — RocksDB, Matchmaker & Cartes Thermiques (En cours & Pipeline Actif ! 🚀)**  
**Préparé pour :** Conseiller IA & Co-fondateurs  
**Préparé par :** Co-fondateur · TaaSim Casablanca  
**État de la Stack :** 12/12 services Docker en ligne (Sains, Actifs, et sous Charge !)

---

## 1. Calibrage du Projet : Semaine & Cartographie des Jalons

Aujourd'hui, **18 mai 2026**, marque le début de la **Semaine 6** de notre projet de fin d'études de 8 semaines.

### 📅 Planification du Calendrier des Sprints
* **Semaines 1–2** → **Sprint 1** (Fondations & Cartographie des Données) — ✅ **100% Terminé**
* **Semaine 3** → **Sprint 2** (Normalisation GPS en Temps Réel) — ✅ **100% Terminé (Vérifié en Direct ! 🚀)**
* **Semaine 4** → **Sprint 3** (Job 2/3 + Cartes Thermiques + Appariement) — 🔶 **En Cours (Pipeline Actif / Jobs Flink 2 & 3 en attente)**
* **Semaine 5** → **Sprint 4** (ETL à Grande Échelle & Analytics) — ⚠️ **0% Terminé (Retard sur le Chemin Critique)**
* **Semaines 6–7** → **Sprint 5** (ML + JWT + ACLs Kafka + HTTPS) — 🔶 **Sprint en cours du calendrier (Non Démarré)**
* **Semaine 8** → **Sprint 6** (Mesures SLA + reprise sur point de contrôle + rapport final + pitch) — ❌ **Non Démarré**

### 📊 Barre de Progression du Développement
```
Axe Chronologique :  [██████████████░░░░░░] Semaine 6 sur 8 (Sprint 5)
Progrès Technique :  [████████░░░░░░░░░░░░] Sprint 2 Entièrement Actif & Opérationnel
```

> [!TIP]
> **Moteur de Flux Activé !** Nous avons lancé avec succès les deux producteurs (GPS et Demandes de Trajets) et soumis le **Job Flink 1 (GpsNormalizer)**. Le flux temps réel Casablanca rémappé est désormais actif et circule de bout en bout !

---

## 2. Liste de Contrôle de l'Implémentation Technique

Voici l'état exact de nos livrables techniques pour l'ensemble des sprints :

### Sprint 1 — Fondations & Cartographie des Données ✅ 100% TERMINÉ
* [x] **Déploiement de la Stack Docker :** Entièrement configuré et exécutant 12 services — `docs_fr/sprint-1/stack-health.png`
* [x] **Configuration du Plan MinIO :** Structure des buckets créée avec `raw/`, `curated/`, `ml/`, et `raw/kafka-archive/`
* [x] **Connectivité de Stockage S3A :** Connecteurs S3A Spark/Flink configurés avec succès pour écrire sur MinIO
* [x] **Remappage Casablanca :** Transformation linéaire des coordonnées implémentée — `notebooks/notebook-spark/01_data_exploration.ipynb`
* [x] **Producteur de GPS de Véhicules :** Moteur de rejeu avec cartographie de la boîte de délimitation de Porto construit — `src/producers/vehicle_gps_producer.py`
* [x] **Init du Schéma Cassandra :** Création de l'espace de clés `taasim` et des tables requises — `db/cassandra_init.cql`
* [x] **Kafka Connect S3 Sink :** Archivage configuré et vérifié — `docs_fr/sprint-1/kafka-connect-s3-archive.md`
* [x] **Producteur de Demandes de Trajets :** Simulateur de demandes clients construit — `src/producers/trip_request_producer.py`

### Sprint 2 — Normalisation GPS en Temps Réel ✅ 100% TERMINÉ (VÉRIFIÉ EN DIRECT)
* [x] **Job Flink 1 (GpsNormalizer) :** Valide, cartographie les zones et anonymise les événements GPS ; écrit dans Cassandra et `processed.gps`. (8 fichiers Java implémentés sous `flink_jobs/`) — **✅ Actif avec l'ID de Job `0c84960f7bd7a460b3b4fdada9231c19`**
* [x] **Gestion des Filigranes (Watermarks) & Points de Contrôle (Checkpoints) :** JAR ombré (shaded) compilé avec des filigranes de retard maximal de 3 minutes (BoundedOutOfOrderness) et des points de contrôle toutes les 60 secondes vers MinIO. — **✅ Vérifié (11+ checkpoints réussis, 0 échec vers `s3a://taasim/.../chk-11`)**
* [x] **Carte Géographique Live des Véhicules sur Grafana :** Déployée via `grafana/provisioning/` en utilisant le connecteur Cassandra et le panneau Geomap.
* [x] **Point de Terminaison Zone de FastAPI :** `/api/v1/vehicles/zone/{zone_id}` entièrement implémenté dans `src/api/main.py` avec des requêtes alignées sur les clés de partition. — **✅ Vérifié comme répondant instantanément avec des véhicules alignés sur les zones !**
* [x] **Vérification de l'Anonymisation GPS :** Projections/anonymisations confirmées — `docs_fr/sprint-2/security-verification.md`. — **✅ Vérifié (les coordonnées dans Cassandra sont parfaitement projetées sur les centroïdes des zones de Casablanca, lat/lon arrondies à 33.55/-7.56)**

### Sprint 3 — RocksDB, Matchmaker & Cartes Thermiques 🔶 EN COURS (PRODUCTEURS ACTIFS)
* [ ] **Job Flink 2 (Demand Aggregator) :** Fenêtre glissante de 30s calculant le ratio offre/demande par zone — *Non démarré*
* [ ] **Job Flink 3 (Trip Matcher) :** Appariement avec état persistant via le backend RocksDB — *Non démarré*
* [ ] **Panneau Heatmap Grafana :** Visualisation dynamique de la densité de la demande dans Grafana — *Non démarré*
* [ ] **Recherche de Zone Adjacente en Repli :** Algorithme de recherche par expansion pour le Job Flink 3 — *Non démarré*

### Sprint 4 — ETL à Grande Échelle & Analytics ⚠️ 0% TERMINÉ (RETARDÉ)
* [ ] **ETL Spark (Porto) :** Traitement par lots des trajectoires de Porto vers Parquet — *Non démarré*
* [ ] **ETL Spark (NYC TLC) :** Agrégation du jeu de données NYC TLC (10M+ lignes/mois) — *Non démarré*
* [ ] **Calculs Hebdomadaires des KPI :** Requêtes analytiques pour les durées moyennes, pics, et déficits de couverture — *Non démarré*
* [ ] **Panneau Tableau KPI Grafana :** Tableau de bord dynamique d'indicateurs clés de performance métier — *Non démarré*

### Sprint 5 — ML, Intelligence & Renforcement de la Sécurité 🔶 SPRINT DU CALENDRIER ACTUEL
* [ ] **Authentification JWT FastAPI :** Sécurisation des API avec rôles d'accès — *Non démarré*
* [ ] **ACLs sur les Topics Kafka :** Sécurisation des partitions du broker Kafka et permissions des producteurs/consommateurs — *Non démarré*
* [ ] **Ingénierie des Caractéristiques Spark ML :** Extraction de caractéristiques temporelles, spatiales, et de décalage (lag features) — *Non démarré*
* [ ] **Entraînement du Modèle GBT :** Arbres de décision boostés par gradient (Gradient Boosted Trees) pour la prévision de demande de taxi — *Non démarré*
* [ ] **Point de Terminaison Prévision de FastAPI :** `POST /api/v1/demand/forecast` répondant en moins de 500ms — *Non démarré*
* [ ] **HTTPS SSL sur FastAPI :** Sécurisation de la passerelle avec certificat auto-signé — *Non démarré*

---

## 3. Diagnostics d'Infrastructure & de Flux (Vérification en Direct)

Un balayage de diagnostic en temps réel a été exécuté sur notre environnement Docker local à **15:45 UTC+01:00**.

### 3.1 Tableau d'État des Conteneurs
Les 12 services du backend sont en ligne et signalés comme sains :

| Service | Conteneur | État | Santé | Port |
| :--- | :--- | :--- | :--- | :--- |
| **Kafka (KRaft)** | `taasim-kafka` | ✅ Actif 3h | `healthy` | `9092` |
| **Kafka UI** | `taasim-kafka-ui` | ✅ Actif 3h | — | `8083 -> 8080` |
| **Kafka Connect** | `taasim-kafka-connect` | ✅ Actif 3h | `healthy` | `8084 -> 8083` |
| **MinIO** | `taasim-minio` | ✅ Actif 3h | `healthy` | `9000/9001` |
| **Cassandra** | `taasim-cassandra` | ✅ Actif 5m | `healthy` | `9042` |
| **Flink JobManager** | `taasim-flink-jm` | ✅ Actif 3h | `healthy` | `8081` |
| **Flink TaskManager** | `taasim-flink-tm` | ✅ Actif 3h | — | — |
| **Spark Master** | `taasim-spark-master` | ✅ Actif 5m | `healthy` | `8080/7077` |
| **Spark Worker** | `taasim-spark-worker` | ✅ Actif 5m | `healthy` | `8082` |
| **Notebook Jupyter** | `taasim-jupyter` | ✅ Actif 5m | `healthy` | `8888` |
| **Grafana** | `taasim-grafana` | ✅ Actif 3h | `healthy` | `3000` |
| **Service FastAPI** | `taasim-api` | ✅ Actif 4m | `healthy` | `8000` |

### 3.2 Intégrations Kafka Connect
Les deux connecteurs d'archivage S3 sont déployés, s'exécutent activement et archivent les événements Kafka sur MinIO :
* `s3-sink-raw-gps` (Statut : **RUNNING** ✅ · Décalage : actif)
* `s3-sink-raw-trips` (Statut : **RUNNING** ✅ · Décalage : actif)

### 3.3 Diagnostics d'Activité des Flux
* **Topics & Offsets Kafka :**
  * `raw.gps` : **12 564 événements bruts** consommés (flux actif)
  * `raw.trips` : **12 910 événements bruts** consommés (flux actif)
  * `processed.gps` : **6 885 événements normalisés** produits par le Job Flink 1 (flux actif)
* **Volumes de Données Cassandra :**
  * `taasim.vehicle_positions` : **1 023 lignes** écrites (en augmentation constante !)
  * `taasim.trips` : **0 ligne** (Le Job 3 n'a pas démarré)
  * `taasim.demand_zones` : **0 ligne** (Le Job 2 n'a pas démarré)
* **Jobs Flink :**
  * **Job 1 (`job1-gps-normalizer`) :** `RUNNING` (JID : `0c84960f7bd7a460b3b4fdada9231c19`)
  * **Points de Contrôle (Checkpoints) :** 143 terminés avec succès, 0 échec. Le checkpoint le plus récent `#143` est stocké sur MinIO : `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/0c84960f7bd7a460b3b4fdada9231c19/chk-143`

---

## 4. Vérification Technique & Audit de Sécurité

### 4.1 Verification de FastAPI
Une requête en direct envoyée à `GET /api/v1/vehicles/zone/15` a renvoyé des données immédiatement :
```json
[
  {
    "taxi_id": "20000007",
    "lat": 33.55,
    "lon": -7.5625,
    "status": "available",
    "event_time": "2026-05-18T16:54:08Z"
  },
  {
    "taxi_id": "20000007",
    "lat": 33.55,
    "lon": -7.5625,
    "status": "available",
    "event_time": "2026-05-18T16:54:06Z"
  }
]
```

### 4.2 Audit d'Anonymisation
Une vérification des logs d'écriture brute dans `taasim.vehicle_positions` confirme que la contrainte de sécurité est respectée.
* Les valeurs de latitude et longitude enregistrées sont strictement projetées sur les centroïdes des zones de Casablanca (ex. `33.55`, `-7.5625`), au lieu d'afficher les coordonnées flottantes de haute précision rejouées à partir des données de Porto. Les coordonnées géographiques brutes ne contournent jamais le normalisateur.

---

## 5. Plan d'Action de Récupération Quotidien

Le Sprint 2 étant validé, nous allons maintenant concentrer nos efforts sur l'implémentation des composants Flink et Spark restants :

| Priorité | Composant | Description de la Tâche | Livrable Prévu |
| :---: | :--- | :--- | :--- |
| 🔴 **P0** | **Sprint 3 (Flink)** | Concevoir et implémenter le **Job Flink 2 (Demand Aggregator)** avec une fenêtre glissante de 30s pour écrire les agrégats d'offre/demande. | Code du Job Flink 2 + build JAR ombré + déploiement sur le JM Flink. |
| 🔴 **P0** | **Sprint 3 (Flink)** | Concevoir et implémenter le **Job Flink 3 (Trip Matcher)** avec RocksDB comme backend d'état pour calculer les appariements et les ETA. | Code du Job Flink 3 + tests de validation d'état. |
| 🟡 **P1** | **Panneau Grafana** | Configurer le panneau de carte thermique (heatmap) de la demande sur Grafana à l'aide des sorties Cassandra du Job Flink 2. | Carte visuelle dynamique de l'offre et de la demande. |
| 🟡 **P1** | **Sprint 4 (Spark)** | Implémenter les notebooks ETL Spark pour les jeux de données CSV Porto et NYC TLC. | Fichiers Parquet stockés dans le stockage MinIO (`curated/`). |

---

```
┌─────────────────────────────────────────────────────────────────────┐
│  TaaSim · Casablanca — Aperçu Quotidien de l'État (18-05-2026)      │
│                                                                     │
│  Calendrier   ██████████████░░░░░░  Semaine 6 sur 8 · Sprint 5      │
│  Progression  ████████░░░░░░░░░░░░  Sprint 2 Entièrement Actif 🚀   │
│  Stack Docker ████████████████████  12/12 Services au Vert          │
│  Jobs Flink   ███████░░░░░░░░░░░░░  1/3 en cours (Job 1 Actif)      │
│  Flux Données ████████████████████  Producteurs Actifs · Offsets ↑  │
│                                                                     │
│  ACTION SUIVANTE → Développer le Job Flink 2 (Demand Aggregator)    │
└─────────────────────────────────────────────────────────────────────┘
```

---
*Rapport généré automatiquement à partir de la télémétrie en direct de l'environnement, se connectant au démon Docker local, aux brokers Kafka, aux nœuds Cassandra, à l'API REST de Flink et à l'état git local.*
