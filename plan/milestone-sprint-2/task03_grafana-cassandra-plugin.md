# task03 — Grafana Cassandra Plugin & Vehicle Position Map

## Context
The live vehicle position map is TaaSim's most compelling real-time demo artefact. Investors and
evaluators see fleet movement on a Casablanca map updating every 10 seconds. This panel also
acts as a live smoke-test for the entire Flink Job 1 → Cassandra pipeline: if vehicles appear on
the map, the end-to-end stream is working. Building it in Sprint 2 provides continuous visual
feedback for the rest of the project.

## Objective
Install the Grafana Cassandra datasource plugin, configure it against the `taasim` keyspace, and
create a live Geomap panel showing all vehicles active in the last 30 seconds, colour-coded by
`status`.

## Acceptance Criteria
- [ ] `HadesArchitect-Cassandra-datasource` plugin installed and visible in Grafana → Plugins
- [ ] Datasource configured: host = `cassandra:9042`, keyspace = `taasim`, consistency = `LOCAL_ONE`
- [ ] Datasource connection test passes (green tick in Grafana UI)
- [ ] **Panel 1 — Vehicle Geomap**: Grafana Geomap panel created with query:
  `SELECT taxi_id, lat, lon, status FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id=? AND event_time > now() - 30s`
  (parametrise zone loop or use ALLOW FILTERING for dev dashboard)
- [ ] Vehicle dots coloured: `available` = green, `assigned` = orange, `offline` = grey
- [ ] Dashboard auto-refreshes every 10 seconds
- [ ] Dashboard JSON exported and committed to `grafana/dashboards/taasim-live.json`

## Technical Hints
- Plugin install via Docker env var: `GF_INSTALL_PLUGINS=hadesarchitect-cassandra-datasource`
  (already set if task01 was followed). Alternatively:
  ```bash
  docker exec grafana grafana-cli plugins install hadesarchitect-cassandra-datasource
  docker restart grafana
  ```
- The Cassandra plugin uses a custom query syntax — refer to the plugin README at
  https://github.com/HadesArchitect/GrafanaCassandraDatasource for the exact query format.
- For the Geomap panel: use `Latitude field` = `lat`, `Longitude field` = `lon`, `Tooltip` = `taxi_id`.
- The `ALLOW FILTERING` clause is acceptable for a development dashboard querying a small
  dataset. In production this would be replaced by per-zone queries.
- Reference: project brief §9.6 Grafana Dashboard (Panel 1).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
