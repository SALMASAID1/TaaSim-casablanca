# task01 — Flink Checkpoint Recovery Test

## Context
TaaSim's reliability requirement states that Flink must resume from its last checkpoint after a
Task Manager failure — not from the beginning of the Kafka topic. This is not theoretical: in
production, containers crash. The checkpoint recovery demonstration is one of the five
non-negotiable Demo Day items in the project brief's evaluation rubric. It must be performed live
(or via screen recording committed to the repo) to pass the Engineering Quality pillar.

## Objective
Kill the Flink Task Manager mid-stream while GPS events are flowing, verify that Job 1 recovers
from the last MinIO checkpoint (not from Kafka topic start), and record the event with
evidence (screen recording or log output).

## Acceptance Criteria
- [ ] All three Flink jobs running with checkpointing active (60-second interval to MinIO)
- [ ] GPS producer running and emitting events continuously during the test
- [ ] **Kill event**: `docker stop flink-taskmanager` while jobs are processing
- [ ] Flink Job Manager logs show job transitioning to RESTARTING state
- [ ] After `docker start flink-taskmanager`, all three jobs resume automatically
- [ ] **Recovery from checkpoint verified**: Kafka consumer group offsets of `flink-job1-gps`
  resume from checkpoint position, not from offset 0 or latest
  (verify with `kafka-consumer-groups.sh --describe --group flink-job1-gps`)
- [ ] **No GPS data gap in Cassandra**: `vehicle_positions` rows continue without a timestamp
  gap larger than 90 seconds (checkpoint interval + recovery time)
- [ ] Recovery time from kill to first new Cassandra write measured and documented (target: < 2 min)
- [ ] Any duplicate events observed (at-least-once semantics) documented in `docs/checkpoint-recovery.md`
- [ ] Screen recording or screenshot sequence committed to `docs/checkpoint-recovery-demo/`

## Technical Hints
- Before killing, note the latest Kafka offset for `raw.gps` partition 0 and the latest
  Cassandra `event_time` in `vehicle_positions`.
- Kill command: `docker stop taasim-flink-taskmanager-1` (container name may vary — check
  with `docker ps`).
- Watch Job Manager UI at `http://localhost:8081` — the job will show as FAILED, then
  RESTARTING, then RUNNING once the Task Manager reconnects.
- To confirm checkpoint-based recovery (not topic-start replay):
  ```bash
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group flink-job1-gps
  ```
  The `CURRENT-OFFSET` should resume close to where it was before the kill, not from 0.
- Flink restart strategy should be configured as `fixed-delay` with `restart-attempts: 3`:
  ```yaml
  # flink-conf.yaml
  restart-strategy: fixed-delay
  restart-strategy.fixed-delay.attempts: 3
  restart-strategy.fixed-delay.delay: 10s
  ```
- Reference: project brief §6.2 Reliability, §8 Evaluation Rubric (Distinction Level — Checkpoint
  recovery demonstrated), §8.1 Demo Day Checklist.

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
