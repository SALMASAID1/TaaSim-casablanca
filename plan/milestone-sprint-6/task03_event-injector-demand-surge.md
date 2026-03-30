# task03 — Event Injector & Live Demo Script

## Context
The Demo Day live demonstration must include a visible demand anomaly: the evaluators need to
see a zone turn red on the heatmap within 60 seconds of an injected event. This is item 5 on
the non-negotiable Demo Day checklist. The `event_injector.py` script is the tool that makes the
demo dramatic — a stadium-exit simulation, a rain event, or a GPS blackout burst all injected on
command during the 20-minute pitch. Rehearsing the full demo script in Sprint 6 ensures no
surprises on Demo Day.

## Objective
Build `producers/event_injector.py` with three configurable anomaly modes (demand spike, GPS
blackout burst, rain event), rehearse the complete live demo script, and confirm the heatmap
responds visibly within 60 seconds of each injection.

## Acceptance Criteria
- [ ] Script `producers/event_injector.py` implemented with CLI interface:
  - `python event_injector.py --mode spike --zone 5 --multiplier 3.0 --duration 300`
  - `python event_injector.py --mode blackout --vehicles 10 --duration 120`
  - `python event_injector.py --mode rain --multiplier 1.4 --duration 600`
- [ ] **Demand spike mode**: for the chosen zone, trip request emission rate multiplied by
  `--multiplier` for `--duration` seconds; simulates a stadium exit or train cancellation
- [ ] **GPS blackout mode**: suppresses GPS events from `--vehicles` randomly selected vehicles
  for `--duration` seconds; those vehicles go offline in Grafana map
- [ ] **Rain mode**: increases global trip request rate by `--multiplier` (default 1.4×) for
  `--duration` seconds
- [ ] **Heatmap response test**: inject spike on zone 5 → confirm `demand_zones` ratio for
  zone 5 increases within 35 seconds (one Job 2 window cycle); confirm Grafana heatmap
  colour intensifies within 60 seconds — timed with a stopwatch and documented
- [ ] Full demo script rehearsed end-to-end and documented in `docs/demo-script.md`:
  1. Start all producers and services
  2. Show live vehicle map (30 s)
  3. Reserve a trip via `curl POST /api/trips` → show match in Cassandra (30 s)
  4. Show demand heatmap baseline (20 s)
  5. Inject morning rush → spike → show heatmap surge (60 s)
  6. Show ML forecast vs actual on Grafana overlay (20 s)
  7. Kill Task Manager → show recovery (40 s)
  8. Show KPI table final numbers (20 s)
- [ ] All three injection modes confirmed working; timings recorded in `docs/demo-script.md`

## Technical Hints
- Spike mode: multiply the `trip_request_producer.py` emission rate for a specific zone by
  sending extra events directly from the injector to `raw.trips` for the spike duration.
  ```python
  import time, uuid
  from confluent_kafka import Producer
  p = Producer({'bootstrap.servers': 'localhost:9092'})
  end = time.time() + args.duration
  while time.time() < end:
      for _ in range(int(args.multiplier * base_rate)):
          event = {"trip_id": str(uuid.uuid4()), "origin_zone": args.zone, ...}
          p.produce("raw.trips", value=json.dumps(event))
      time.sleep(1)
  ```
- Blackout mode: maintain a set of `suppressed_taxi_ids`; the GPS producer checks this set
  before sending. The injector writes to a shared Redis key or a simple file flag.
  Simpler alternative: publish GPS events with `status=offline` for those vehicles.
- Rain mode: publish a control event to a `control.events` Kafka topic; the trip request
  producer subscribes and adjusts its multiplier dynamically.
- Reference: project brief §2.3 Real-Time Simulation Layer (event_injector.py row),
  §8.1 Demo Day Checklist (point 5).

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
