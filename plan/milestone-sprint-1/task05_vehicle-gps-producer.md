# task05 — Vehicle GPS Producer

## Context
Real-time Casablanca GPS feeds do not exist. The streaming pipeline must be fed by a simulator
that replays authentic Porto trajectories at accelerated speed through Kafka. This script is the
entry point of TaaSim's entire real-time data flow — without it, Flink Jobs 1, 2, and 3 have no
input. The deliberate GPS noise and blackout simulation are also required so that Sprint 2 can
test watermark handling with realistic late-event conditions.

## Objective
Build `producers/vehicle_gps_producer.py` that replays Porto GPS polylines at 10× speed through
Kafka topic `raw.gps`, adding Gaussian coordinate noise and simulating 5% per-vehicle blackout
probability.

## Acceptance Criteria
- [ ] Script reads Porto `train.csv` from `s3a://taasim/raw/porto-trips/` (or local path via CLI arg)
- [ ] Kafka messages published to topic `raw.gps` with key = `taxi_id`
- [ ] Payload fields present: `taxi_id`, `timestamp` (event time ISO-8601), `lat`, `lon`, `speed`, `status`
- [ ] Coordinates have been transformed to Casablanca bounding box (reuses task04 function)
- [ ] Gaussian noise applied: σ ≈ 0.0002 degrees (≈ 20 m)
- [ ] Blackout simulation: with 5% probability per vehicle per event, Kafka send is delayed by
  60–180 seconds (random uniform), producing out-of-order events in the topic
- [ ] Replay speed configurable via `--speed` CLI argument (default: 10×)
- [ ] `kafka-console-consumer --topic raw.gps` shows valid JSON messages flowing
- [ ] Script is runnable with `python producers/vehicle_gps_producer.py --speed 10`

## Technical Hints
- Parse Porto `POLYLINE` column (JSON array of `[lon, lat]` pairs) and iterate coordinates.
- Use `confluent-kafka` Python client for Kafka producer:
  ```python
  from confluent_kafka import Producer
  p = Producer({'bootstrap.servers': 'localhost:9092'})
  p.produce('raw.gps', key=taxi_id, value=json.dumps(event))
  ```
- For the 10× speed: record the original Porto event timestamps; compute inter-event sleep
  duration divided by the speed multiplier.
- Blackout implementation: use `random.random() < 0.05` per event; if True, schedule the send
  with `time.sleep(random.uniform(60, 180))` in a separate thread so other vehicles continue.
- Kafka message key as `taxi_id` ensures all GPS events for the same vehicle go to the same
  partition → preserves per-vehicle ordering within a partition.
- Reference: project brief §2.3 Real-Time Simulation Layer, §9.2 Data Producers.

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
