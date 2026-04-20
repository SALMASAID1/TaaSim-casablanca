# task05 — GPS Anonymisation Verification

## Context
Privacy regulation and the project brief (§6.3 Security) both require that raw GPS coordinates
are never persisted. Flink Job 1 is supposed to snap `lat/lon` to zone centroid before writing to
Cassandra — but this must be verified by an automated test, not just assumed. A future code
change could accidentally re-introduce raw coordinates. This task creates the test that catches it.

## Objective
Write an integration test that confirms raw GPS coordinates published to `raw.gps` are never
stored verbatim in Cassandra `vehicle_positions` — only zone centroid coordinates appear.

## Acceptance Criteria
- [ ] Integration test script `tests/test_anonymisation.py` (or shell script) committed
- [ ] Test publishes 20 GPS events with known precise coordinates to Kafka `raw.gps`
- [ ] Test waits for Flink Job 1 to process the events (poll Cassandra with timeout ≤ 30 s)
- [ ] Test asserts that **no row** in `vehicle_positions` has coordinates matching any of the 20
  raw input coordinates (within 1 m tolerance)
- [ ] Test asserts that **all 20 rows** have coordinates matching one of the 16 known zone centroid
  coordinates (within 1 m tolerance)
- [ ] Test passes cleanly when run with `pytest tests/test_anonymisation.py`
- [ ] Result summary added to `docs/sprint-2/security-verification.md`

## Technical Hints
- Zone centroid coordinates are derived from `zone_mapping.csv` bounding boxes as midpoints.
  Build a dict `{zone_id: (centroid_lat, centroid_lon)}` using `(lat_min + lat_max)/2` and
  `(lon_min + lon_max)/2`.
- To check coordinate equality with tolerance, use:
  ```python
  import math
  def approx_equal(a, b, tol_m=1.0):
      # Haversine or simple degree tolerance
      return abs(a[0]-b[0]) < tol_m/111000 and abs(a[1]-b[1]) < tol_m/111000
  ```
- For Cassandra polling in the test: use a `for _ in range(30): ... time.sleep(1)` retry loop
  rather than a fixed sleep, to keep test fast when the pipeline is healthy.
- Keep test events outside the normal producer stream (use distinct `taxi_id` values like
  `test-vehicle-01` … `test-vehicle-20`) so they're easy to query and clean up.
- Reference: project brief §6.3 Security (GPS Anonymization row), §3.3 Flink Jobs (Job 1).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
