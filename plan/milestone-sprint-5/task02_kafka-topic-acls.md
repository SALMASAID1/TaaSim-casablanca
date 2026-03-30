# task02 — Kafka Topic ACLs

## Context
Kafka topics carry raw GPS coordinates and trip reservation data. Without access controls, any
process — including a buggy producer script — could write to `processed.demand` and corrupt
the demand signal that drives the heatmap and ML overlay. The project brief requires producers
to write only to `raw.*` topics, Flink jobs to read `raw.*` and write `processed.*`, and
`processed.demand` to be admin-only for reads. This task enforces those boundaries.

## Objective
Configure Kafka Topic ACLs so that each service can only produce or consume topics it is
authorised for, and document the ACL configuration so it can be reproduced and verified.

## Acceptance Criteria
- [ ] Kafka configured with `allow.everyone.if.no.acl.found=false` (deny-by-default)
- [ ] ACL rule: GPS producer (`principal=User:gps-producer`) — WRITE on `raw.gps` only
- [ ] ACL rule: Trip producer (`principal=User:trip-producer`) — WRITE on `raw.trips` only
- [ ] ACL rule: Flink jobs (`principal=User:flink`) — READ on `raw.*`, WRITE on `processed.*`
- [ ] ACL rule: Admin principal — full access to all topics
- [ ] ACL rule: `processed.demand` — READ restricted to `User:admin` only
- [ ] Verification: `kafka-acls.sh --bootstrap-server localhost:9092 --list` output captured and
  committed to `docs/kafka-acls-list.txt`
- [ ] Verification: attempt to write from `User:gps-producer` to `processed.demand` returns an
  authorization error (logged and documented)
- [ ] All Kafka producer and consumer scripts updated to use the correct principal credentials

## Technical Hints
- Enable ACLs in `kafka/config/kraft/server.properties`:
  ```properties
  authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
  allow.everyone.if.no.acl.found=false
  super.users=User:admin
  ```
- Create ACL via CLI:
  ```bash
  # Allow GPS producer to write raw.gps
  kafka-acls.sh --bootstrap-server localhost:9092 \
    --add --allow-principal User:gps-producer \
    --operation Write --topic raw.gps

  # Allow Flink to read all raw.* topics
  kafka-acls.sh --bootstrap-server localhost:9092 \
    --add --allow-principal User:flink \
    --operation Read --topic "raw.*" --resource-pattern-type prefixed
  ```
- For KRaft mode with SASL, use `SASL_PLAINTEXT` listener and configure
  `KAFKA_OPTS=-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf` in Docker.
- Alternatively, for a simpler dev setup, enforce ACLs by network segmentation (only known
  containers can reach Kafka) and document this as an acknowledged dev shortcut in the ADR.
- Reference: project brief §6.3 Security (Kafka Topic ACLs row).

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
