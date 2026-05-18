## Plan: Canonical Affine Mapping + Zone Quality Gates

Upgrade the Spark batch ETLs to follow ADR‑01 (relative-position bbox affine mapping) and add explicit data-quality metrics/gates so bad mapping can’t be silently “fixed” by random zone assignment. In Porto ETL, also capture origin+destination (start/end) without exploding all GPS points.

**Steps**
1. Porto ETL: replace shift mapping with ADR‑01 affine bbox mapping.
   - In `spark_jobs/etl_porto.py`, remove `LAT_SHIFT`/`LON_SHIFT` usage and implement a reusable Column-expression helper (no UDF) that maps `(lon, lat)` via relative position within the Porto bbox to the same relative position within the Casablanca bbox.
   - Use Porto bbox from task04 hints (lon [-8.7, -8.5], lat [41.1, 41.2]).
   - Use Casablanca bbox as the union of `metadata/zone_mapping.csv` (lon [-7.730, -7.480], lat [33.510, 33.645]).
   - Clamp rel_lon/rel_lat to [0,1] (to match Notebook 03) but ALSO track clamp rate as a quality metric.

2. Porto ETL: extract origin+destination points without exploding.
   - Keep the performance design (1 row per trip).
   - From parsed `coords`, extract:
     - origin raw: first point
     - destination raw: last point (use `element_at(coords, -1)` pattern)
   - Map both origin and destination via ADR‑01.
   - Maintain backward compatibility:
     - Keep `lon`/`lat` as origin mapped coordinates.
     - Keep `arrondissement_id`/`zone_name`/`zone_type` as origin zone fields.
   - Add new columns for destination (and optionally origin-prefixed fields for clarity):
     - `origin_lon`, `origin_lat`, `origin_arrondissement_id`, `origin_zone_name`, `origin_zone_assignment_method`
     - `dest_lon`, `dest_lat`, `dest_arrondissement_id`, `dest_zone_name`, `dest_zone_assignment_method`
     - `zone_assignment_method` (for the legacy zone) mirroring origin.

3. Porto ETL: remove hash fallback; replace with explicit out_of_bounds.
   - Delete the `hash(TRIP_ID) % 16 + 1` fallback.
   - After each bbox join (origin + dest), if no match:
     - set zone id to 0
     - set zone_name to `Out-of-bounds`
     - set assignment method to `out_of_bounds`
   - If matched:
     - assignment method `bbox`

4. Porto ETL: add audit metrics + hard failure gates.
   - Compute metrics in a single aggregation action on the cached final DataFrame (avoid multiple full scans):
     - total trips
     - origin out_of_bounds count + rate
     - destination out_of_bounds count + rate
     - origin clamped count + rate
     - destination clamped count + rate
   - Add thresholds as constants near the top (tunable):
     - `MAX_OUT_OF_BOUNDS_RATE` (recommended default 0.01)
     - `MAX_CLAMP_RATE` (recommended default 0.01; warn or fail depending on desired strictness)
   - If out_of_bounds rate exceeds threshold for origin or destination: raise an exception on the driver to fail the job.

5. NYC ETL: add zone-join audit gate (safety check).
   - In `spark_jobs/etl_nyc_tlc.py` inside `apply_zone_mapping()`, after the broadcast join:
     - compute count of rows where `arrondissement_id` or `zone_name` is null
     - fail the job if unmatched > 0
     - log zone table row count (expect 16)

6. Documentation updates.
   - Update `docs/sprint-4/status-report.md` to note:
     - Porto ETL now uses ADR‑01 affine mapping (not a constant shift)
     - hash fallback removed; out_of_bounds introduced
     - quality gates added
     - NYC ETL includes zone mapping audit gate
   - Update `docs/status-sync-report.md` to reflect:
     - Spark Porto ETL aligns with “CSV mode” affine mapping (producer) and explicitly tracks out_of_bounds instead of hashing
   - Add a short note in `plan/milestone-sprint-1/task04_porto-casablanca-coordinate-transform.md` clarifying:
     - production ETL keeps 1 row per trip (origin/dest) for performance
     - full explode-per-GPS-point remains a validation/notebook approach

**Relevant files**
- `spark_jobs/etl_porto.py` — Step 3 mapping, Step 5 zone join, remove hash fallback, add origin/dest columns, add audit+gates.
- `spark_jobs/etl_nyc_tlc.py` — `apply_zone_mapping()` add null-join audit and failure condition.
- `metadata/zone_mapping.csv` — source of Casablanca bbox union and zone joins.
- `docs/sprint-4/status-report.md` — document new quality gates + mapping.
- `docs/status-sync-report.md` — align batch ETL description with producer mapping.
- `plan/milestone-sprint-1/task04_porto-casablanca-coordinate-transform.md` — add note about production vs validation approach.

**Verification**
1. Run `spark-submit` for Porto ETL and confirm:
   - job completes under SLA
   - out_of_bounds rate is ~0 and below threshold
   - clamp rate is low and logged
   - output schema contains legacy columns + new origin/dest columns
2. Run `spark_jobs/kpi_weekly.py` and confirm it still computes KPIs (no schema break).
3. Run `spark-submit` for NYC ETL and confirm zone-join audit passes (0 unmatched).
4. Spot-check output:
   - sample a few rows, verify origin/dest coords lie within Casablanca bbox union
   - verify `arrondissement_id=0` rows exist only if genuinely unmatched (and would trigger gate if too frequent)

**Decisions**
- Scope: implement changes in Porto ETL, add NYC audit gate, and update docs (per user selection).
- Unmatched zones: replace hash fallback with explicit `out_of_bounds` and fail if too frequent.
- Target bbox: use union bbox from `metadata/zone_mapping.csv` for maximum zone-join success.

**Further Considerations**
1. Threshold strictness: if you want “never allow out_of_bounds,” set `MAX_OUT_OF_BOUNDS_RATE = 0.0` once the pipeline is stable.
2. If you later want street-level fidelity, keep using Notebook 03 outputs as a separate curated dataset rather than embedding routing in Spark ETL.