# Vérification de Sécurité — Anonymisation GPS (Sprint 2)

## Objectif
Vérifier que les **coordonnées GPS brutes (lat/lon)** publiées dans le topic `raw.gps` ne sont **jamais persistées** dans Cassandra.

## Méthode
- Publier un petit lot d'événements GPS contenant des coordonnées brutes connues.
- Attendre que le Job Flink 1 traite ces événements.
- Interroger la table Cassandra `taasim.vehicle_positions`.

## Assertions (Contrôles)
- [x] Aucune ligne stockée ne correspond à une coordonnée brute d'entrée (tolérance ≤ 1m).
- [x] Toutes les coordonnées stockées correspondent à l'un des 16 centroïdes de zone définis (tolérance ≤ 1m).

## Preuves d'exécution
- [x] Sortie du test (pytest) journalisée/rattachée.
- [x] Résultats des requêtes d'exemple Cassandra.
