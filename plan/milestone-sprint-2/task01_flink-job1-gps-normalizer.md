# task01 — Flink Job 1: GPS Normalizer

## Context
Flink Job 1 is the entry point of TaaSim's real-time pipeline. Every GPS event published by the
vehicle producer flows through this job before anything else sees it. It validates coordinates,
assigns each ping to a Casablanca arrondissement, anonymises the precise location to a zone
centroid, and writes clean positions to Cassandra. It also forwards normalised events to the
`processed.gps` topic consumed by Job 2. A single missed configuration here (e.g. wrong bbox,
missing zone mapping) cascades into every downstream component.

## Objective
Implement `flink_jobs/job1_gps_normalizer.py` (or Java equivalent) that consumes `raw.gps`,
validates and zone-maps each GPS event, anonymises coordinates, and writes to Cassandra
`vehicle_positions` and Kafka `processed.gps`.

## Acceptance Criteria
- [ ] Flink job reads from Kafka topic `raw.gps` using a named consumer group `flink-job1-gps`
- [ ] Coordinate validation: events with `lat/lon` outside Casablanca bbox (lon: 7.4°W–7.8°W,
  lat: 33.4°N–33.7°N) are filtered out and counted in a side metric
- [ ] Events with `speed > 150 km/h` are discarded
- [ ] Zone mapping: each valid event receives an `arrondissement_id` (1–16) via broadcast state
  lookup against `zone_mapping.csv`
- [ ] **Anonymisation enforced**: raw `lat/lon` replaced with zone centroid coordinates before any
  sink — raw coordinates are never written to Cassandra (verified in task05)
- [ ] Events written to Cassandra `taasim.vehicle_positions` (upsert / append)
- [ ] Normalised events forwarded to Kafka topic `processed.gps`
- [ ] Job runs stably for 10 minutes with the GPS producer active (no restarts, no exceptions)
- [ ] Watermarks assigned (see task02 for detailed watermark config)

## Technical Hints
- Use PyFlink's `StreamExecutionEnvironment` with `KafkaSource` (Flink 1.17+):
  ```python
  from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
  source = KafkaSource.builder() \
      .set_bootstrap_servers("kafka:9092") \
      .set_topics("raw.gps") \
      .set_group_id("flink-job1-gps") \
      .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
      .set_value_only_deserializer(SimpleStringSchema()) \
      .build()
  ```
- Broadcast state pattern for zone mapping: read `zone_mapping.csv` into a `MapStateDescriptor`
  and broadcast it; in the `BroadcastProcessFunction`, use the bounding-box table to look up
  `arrondissement_id` for each event.
- Anonymisation: after zone lookup, replace `lat = zone_centroid_lat`, `lon = zone_centroid_lon`.
  Zone centroid coordinates are derived from `zone_mapping.csv` as the midpoint of the bbox:
  `centroid_lat = (lat_min + lat_max)/2`, `centroid_lon = (lon_min + lon_max)/2`.
- Cassandra sink: use `CassandraSink.addSink(stream)` with the `taasim.vehicle_positions` table.
  Set `enableWriteAheadLog()` for at-least-once semantics.
- Reference: project brief §3.3 Flink Processing Jobs (Job 1), §9.3 Flink Jobs.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
