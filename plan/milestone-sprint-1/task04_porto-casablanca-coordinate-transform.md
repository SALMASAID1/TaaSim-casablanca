# task04 — Porto-Casablanca Coordinate Transform

## Context
No public open dataset exists for Casablanca taxi trips. TaaSim uses Porto taxi trajectories as a
proxy, but the GPS coordinates sit in Portugal. A linear bounding-box transform moves them into
the Casablanca geographic frame while preserving all trip-length distributions, temporal demand
curves, and call-type patterns — the properties that make the data useful. This remapping is the
first real data engineering task and must be validated visually before the streaming pipeline
consumes any coordinates.

## Objective
Develop and validate a PySpark coordinate transformation that linearly maps Porto GPS bounding
box to the Casablanca bounding box, producing a dataset whose coordinates visually fall within
Casablanca arrondissements on an OpenStreetMap overlay.

## Acceptance Criteria
- [x] PySpark function `transform_coordinates(lon, lat) → (cas_lon, cas_lat)` implemented and
  unit-tested with at least 5 boundary-point assertions
- [x] Full Porto `train.csv` processed: new columns `cas_lon`, `cas_lat`, `arrondissement_id`
  added to each GPS point row
- [x] Zone assignment joins correctly against `zone_mapping.csv` (provided in starter kit) — every
  GPS point receives a valid `arrondissement_id` (1–16) or is tagged `out_of_bounds`
- [x] Validation plot: scatter of 10 000 sampled transformed coordinates overlaid on Casablanca
  OSM map — saved as `docs/sprint-1/casablanca-coordinate-validation.png`
- [x] Jupyter notebook `notebooks/notebook-spark/01_data_exploration.ipynb` committed with: schema inspection,
  trip duration distribution histogram, call-type breakdown (A/B/C), temporal demand curve
  (trips per hour of day)

## Technical Hints
- Porto bounding box: longitude 8.5°W – 8.7°W, latitude 41.1°N – 41.2°N.
- Casablanca bounding box: longitude 7.4°W – 7.8°W, latitude 33.4°N – 33.7°N.
- Linear transform formula:
  ```python
  cas_lon = cas_lon_min + (porto_lon - porto_lon_min) / (porto_lon_max - porto_lon_min) \
            * (cas_lon_max - cas_lon_min)
  # same pattern for lat
  ```
- Porto `POLYLINE` field is a JSON string: `"[[lon1,lat1],[lon2,lat2],...]"`. Use
  `from_json` + `explode` in PySpark to produce one row per GPS point.
- For the OSM validation plot, use `folium` (Python) or `geopandas` with the OpenStreetMap
  Casablanca extract from https://download.geofabrik.de/africa/morocco.html
- Reference: project brief §2.1 Porto Taxi Trajectories, §9.2 Data Producers.

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [x] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
