# task02 — Watermark Strategy & Flink Checkpointing

## Context
The GPS producer deliberately emits out-of-order events with up to 3 minutes of delay and
periodic 60–180 second blackouts (see Sprint 1 task05). A processing-time approach to
aggregation will produce measurably incorrect demand counts — this is verified during the Week 8
evaluation. Watermarks make late-event handling concrete and testable. Checkpointing to MinIO
guarantees that if the Task Manager crashes, Job 1 resumes from its last consistent state rather
than reprocessing from the Kafka topic beginning.

## Objective
Configure `BoundedOutOfOrdernessWatermarks` with 3-minute max lateness on the GPS stream,
enable Flink checkpointing every 60 seconds to MinIO, and produce documented evidence that a
3-minute-late event is correctly handled (included in the window, not dropped).

## Acceptance Criteria
- [ ] `BoundedOutOfOrdernessWatermarks` assigned with `maxOutOfOrderness = Duration.ofMinutes(3)`
- [ ] Checkpointing enabled: interval = 60 seconds, mode = `AT_LEAST_ONCE`,
  checkpoint storage = `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`
- [ ] RocksDB state backend configured for Job 1
- [ ] Late-event test: inject a GPS event with timestamp 2 minutes in the past → confirm the event
  is processed (appears in Cassandra) and not silently dropped
- [ ] Late-event test: inject a GPS event with timestamp 4 minutes in the past → confirm the event
  is dropped (beyond allowed lateness) and a side-output counter increments
- [ ] Test results documented in `docs/sprint-2/watermark-test-evidence.md` with log snippets or screenshots
- [ ] Checkpoint directory `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/` populated after 60 seconds of
  job run (verify with `mc ls`)

## Technical Hints
- Java implementation reference (this repo): `flink_jobs/src/main/java/com/taasim/flink/job1/Job1GpsNormalizer.java`
  - Watermarks: `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMinutes(3))`
  - Checkpointing: `env.enableCheckpointing(60_000)` and `setCheckpointStorage("s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/")`
- Watermark assignment in PyFlink:
  ```python
  from pyflink.common.watermark_strategy import WatermarkStrategy
  from pyflink.common import Duration

  strategy = WatermarkStrategy \
      .for_bounded_out_of_orderness(Duration.of_minutes(3)) \
      .with_timestamp_assigner(MyTimestampAssigner())

  stream = env.from_source(source, strategy, "GPS Source")
  ```
- Checkpoint configuration:
  ```python
  env.enable_checkpointing(60_000)  # 60 seconds in ms
  env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.AT_LEAST_ONCE)
  env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
  env.set_state_backend(EmbeddedRocksDBStateBackend())
  env.get_checkpoint_config().set_checkpoint_storage("s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/")
  ```
- To inject a late event for testing, produce a Kafka message to `raw.gps` with a `timestamp`
  field set to `now - 2 minutes` (ISO-8601). Use `kafka-console-producer` or a small Python script.
- Side output for dropped late events: use `OutputTag` and `getSideOutput()`.
- Reference: project brief §2.3 Real-Time Simulation Layer (Engineering Constraint box),
  §6.2 Reliability, §9.3 Flink Jobs.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
