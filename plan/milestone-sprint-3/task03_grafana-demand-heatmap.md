# task03 — Grafana Demand Heatmap Panel

## Context
The demand heatmap is the most visually impactful panel in TaaSim's dashboard. It shows
investors at a glance which arrondissements are under-served (high pending requests, few
vehicles) in real time. It is also the panel that makes the anomaly injection in Sprint 6 dramatic:
when the event injector triggers a stadium-exit demand spike, a zone turns deep red on the
heatmap within 60 seconds. This panel reads from Flink Job 2's output in Cassandra.

## Objective
Create a Grafana Geomap heatmap panel that visualises per-zone supply/demand ratio from
`demand_zones`, updating automatically every 10 seconds, with colour intensity proportional to
`pending_requests / active_vehicles`.

## Acceptance Criteria
- [ ] **Panel 2 — Demand Heatmap**: Geomap panel with heatmap layer type added to the
  `taasim-live` dashboard
- [ ] Query: `SELECT zone_id, lat, lon, pending_requests, active_vehicles, ratio FROM
  taasim.demand_zones WHERE city='casablanca' AND zone_id=? AND window_start > now()-2min`
  (or equivalent that retrieves all zones' latest window)
- [ ] Colour intensity mapped to `ratio` field: low ratio (< 0.5) = cool blue, high ratio (> 2.0) = red
- [ ] Panel auto-refreshes every 10 seconds (inherits dashboard refresh interval)
- [ ] Each zone dot positioned at its centroid coordinates (from `zone_mapping.csv`)
- [ ] Tooltip shows: zone name, active vehicles count, pending requests count, ratio
- [ ] Heatmap visibly updates within 35 seconds of new data arriving in `demand_zones`
- [ ] Dashboard JSON re-exported to `grafana/dashboards/taasim-live.json` after panel added

## Technical Hints
- Grafana Geomap heatmap layer: in panel settings → Layers → Add layer → type = Heatmap.
  Set the weight field to `ratio`.
- Zone centroid lat/lon: join in Cassandra (add `lat` and `lon` columns to `demand_zones` when
  writing from Flink Job 2, using centroid values from `zone_mapping.csv`), or enrich in Grafana
  via a `Transformation → Lookup fields from frame` using a static zone reference CSV.
- For colour thresholds: in Grafana panel → Standard options → Thresholds:
  - 0.0 → blue (balanced)
  - 1.0 → yellow (mild pressure)
  - 2.0 → red (high demand)
- Reference: project brief §9.6 Grafana Dashboard (Panel 2).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
