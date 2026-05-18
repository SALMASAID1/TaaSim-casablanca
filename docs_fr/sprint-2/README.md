# Sprint 2 (Semaine 3) — Traitement de Flux I (GPS)

Ce dossier contient les **preuves du Sprint 2** requises par le descriptif officiel (Semaine 3) :

- Le Job Flink 1 en cours d'exécution avec les points de contrôle (checkpoints) activés (MinIO)
- Les résultats des tests de filigranes (watermarks) / événements tardifs (late events)
- La carte en direct des positions des véhicules sur Grafana (source de données Cassandra)
- Le contrôle de sécurité : anonymisation GPS vérifiée (les coordonnées géographiques brutes ne sont jamais persistées)

## Index des Preuves

- Contrats du Job 1 (entrées/sorties/configuration) : `job1-contract.md`
- Notes de tests des filigranes & des points de contrôle : `watermark-test-evidence.md`
- Notes de vérification de l'anonymisation GPS : `security-verification.md`

## Tâches du Plan Associées

- `plan/milestone-sprint-2/task01_flink-job1-gps-normalizer.md`
- `plan/milestone-sprint-2/task02_watermark-and-checkpointing.md`
- `plan/milestone-sprint-2/task03_grafana-cassandra-plugin.md`
- `plan/milestone-sprint-2/task04_fastapi-boilerplate-zone-endpoint.md`
- `plan/milestone-sprint-2/task05_gps-anonymization-verification.md`
