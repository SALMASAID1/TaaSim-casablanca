# Sprint 1 — Pack de Preuves

Ce dossier contient les fichiers de preuves pour la **Semaine 1 / Sprint 1** requis par le descriptif officiel.

## Contenu du dossier

- `stack-health.png`, `stack-health.txt` — Preuve du fonctionnement de la stack Docker Compose (Kafka, MinIO, Cassandra, Flink, Spark, Grafana, Kafka Connect)
- `minio-layout.md` — Structure du bucket MinIO (`raw/`, `curated/`, `ml/`, `raw/kafka-archive/`) + preuve de l'alimentation initiale (seeding)
- `s3a-connector-setup.md` — Configuration du connecteur S3A de Spark + Flink et tests de fumée (smoke tests) en lecture/écriture
- `kafka-connect-s3-archive.md` — Configurations des connecteurs S3 Sink de Kafka Connect + preuve des objets écrits dans `raw/kafka-archive/`
- `casablanca-coordinate-validation.png` — Graphique de validation du remappage des coordonnées de Porto vers Casablanca
- `casablanca-interactive-locked.html` — Carte interactive Folium (boîte de délimitation verrouillée) utilisée pour la validation visuelle

## Notebook associé
Le travail d'exploration et les graphiques sont générés dans le notebook `notebooks/notebook-spark/01_data_exploration.ipynb`.
