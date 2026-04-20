# Watermark & Checkpointing Evidence (Sprint 2)

## Goal
Demonstrate **event-time processing** with a **3-minute allowed lateness watermark** and **Flink checkpointing to MinIO**.

## Setup
- Flink Job 1 deployed and running
- Checkpointing interval: 60s
- Checkpoint storage: MinIO (S3A)
- Checkpoint directory: `s3a://taasim/raw/kafka-archive/flink-checkpoints/job1/`

## Test Cases

### Case A — Late by 2 minutes (should be processed)
- Inject a GPS event with `timestamp = now - 2 minutes`
- Expected: event is processed and appears in Cassandra

### Case B — Late by 4 minutes (should be dropped)
- Inject a GPS event with `timestamp = now - 4 minutes`
- Expected: event is dropped and the late-event counter increments

## Evidence
- [ ] Flink UI screenshot showing checkpoints completed
- [ ] MinIO listing showing checkpoint directory populated
- [ ] Log snippets showing Case A processed and Case B dropped
