# Documentation (Docs)

Ce dossier contient les preuves écrites et les dossiers de décision d'architecture (ADR) du projet.

## Sprint 1 (Semaine 1) — Fondations & Cartographie des Données
Les preuves et notes de configuration sont regroupées sous `docs_fr/sprint-1/` :

- [Capture d'écran de l'état de la stack](sprint-1/stack-health.png) (+ texte brut : [stack-health.txt](sprint-1/stack-health.txt))
- [Preuve de la structure des buckets MinIO](sprint-1/minio-layout.md)
- [Configuration & tests de fumée S3A (Spark + Flink)](sprint-1/s3a-connector-setup.md)
- [Preuve d'archivage Kafka Connect → MinIO](sprint-1/kafka-connect-s3-archive.md)
- [Graphique de validation de remappage de coordonnées Porto → Casablanca](sprint-1/casablanca-coordinate-validation.png)
- [Validation interactive de la carte de Casablanca (Folium)](sprint-1/casablanca-interactive-locked.html)

## Documentation du Pipeline de Données
- [Pipeline Spark de Génération Synthétique de Trajets](spark_pipeline_documentation.md) — Vue d'ensemble détaillée de la logique de simulation de Casablanca.

## ADRs (Dossiers de Décision d'Architecture)
- [ADR-001 — Schéma Cassandra & clés de partitionnement](adr/adr-001-cassandra-schema.md)

## Sprint 2 (Semaine 3) — Traitement de Flux I (GPS)
Les preuves sont regroupées sous `docs_fr/sprint-2/` :

- [Index du Sprint 2](sprint-2/README.md)
