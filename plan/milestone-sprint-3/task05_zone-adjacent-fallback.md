# task05 — Zone Adjacent-Zone Fallback in Job 3

## Context
In low-density hours or underserved peripheral arrondissements (Bouskoura, Ain Sebaâ, Sidi
Moumen), no vehicle may be available in the requested zone. Without a fallback, Job 3 times out
and emits an `unmatched` event every time — an unacceptably high unmatch rate for the demo.
The 5-second adjacent-zone expansion makes the matching logic realistic: first search the exact
zone, then widen to neighbours if necessary, and only emit `unmatched` after exhausting adjacent
zones within the SLA window.

## Objective
Extend Job 3's matching logic so that if no available vehicle exists in the requested zone within
5 seconds, the search expands to adjacent zones using the adjacency list in `zone_mapping.csv`,
and an `unmatched` event is only emitted if all adjacent zones are also empty.

## Acceptance Criteria
- [ ] `zone_mapping.csv` adjacency list loaded into Job 3 as broadcast state or static lookup
- [ ] Fallback logic: if `same_zone_vehicles == 0` after 5-second timer, query adjacent zones
  in order of proximity (ascending distance between centroids)
- [ ] If a vehicle is found in an adjacent zone: match event emitted with `matched_zone ≠
  origin_zone` flag set to `true`; ETA recalculated using actual inter-zone distance
- [ ] `unmatched` event emitted to `raw.unmatched` only if **all** adjacent zones are also empty
- [ ] Unit test: simulate a trip request to zone 7 with no vehicles in zone 7 but one vehicle in
  adjacent zone 8 → assert match returned with `matched_zone=8`
- [ ] Unit test: simulate a trip request to zone 7 with no vehicles in any adjacent zone →
  assert `unmatched` event emitted
- [ ] Unmatched rate observable in Grafana (add a simple KPI counter to `taasim-live` dashboard)

## Technical Hints
- Load adjacency list at job startup from `zone_mapping.csv` into a Python dict:
  `{zone_id: [adjacent_zone_id_1, adjacent_zone_id_2, ...]}`.
  Broadcast it as `MapState` if using `BroadcastProcessFunction`.
- Centroid distance between zones (for ETA and adjacency ordering):
  ```python
  import math
  def haversine_km(lat1, lon1, lat2, lon2):
      R = 6371
      dlat = math.radians(lat2 - lat1)
      dlon = math.radians(lon2 - lon1)
      a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) \
          * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
      return R * 2 * math.asin(math.sqrt(a))
  ```
- Adjacent zone search should still respect the 5-second SLA window — do not restart the timer.
- Reference: project brief §3.3 Flink Processing Jobs (Job 3 row), §9.3 Flink Jobs (Job 3 hints).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
