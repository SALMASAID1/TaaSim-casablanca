# task05 — Technical Report (12–15 Pages)

## Context
The technical report is worth 25% of the final grade. The brief requires it to cover architecture,
dataset remapping, ML evaluation with baseline comparison, an honest post-mortem on failures,
the NFR measurement table, and ADRs for all major decisions. Teams that write a post-mortem
acknowledging real failures score higher than teams that omit difficulties — the rubric
explicitly rewards honesty. The report must be submitted on Demo Day alongside the pitch deck.

## Objective
Write and submit a 12–15 page technical report covering all required sections as specified in the
project brief's evaluation rubric, including honest documentation of failures and real measured
metrics.

## Acceptance Criteria
- [ ] Report length: 12–15 pages (excluding cover page, table of contents, and references)
- [ ] **Section 1 — Architecture**: full system diagram, Kappa vs Lambda justification, component
  roles table (one row per service in the tech stack)
- [ ] **Section 2 — Dataset Remapping**: Porto → Casablanca coordinate transform methodology,
  zone mapping table (all 16 arrondissements), validation plot (`docs/casablanca-coordinate-validation.png`)
- [ ] **Section 3 — ML Evaluation**: per-zone RMSE comparison table (GBT model vs naive 7-day
  baseline), feature importance chart, explanation of top 3 predictors in business terms
- [ ] **Section 4 — NFR Measurement**: completed SLA table from task02 with actual measured
  values (copy from `docs/sla-measurement-table.md`)
- [ ] **Section 5 — ADRs**: Architecture Decision Records for:
  - ADR-001: Cassandra schema and partition key choices
  - ADR-002: Kappa Architecture vs Lambda (why no Spark Streaming)
  - ADR-003: GBT over other ML algorithms (why not ARIMA, Prophet, or LSTM)
  - ADR-004: Any significant implementation decision made during the project
- [ ] **Section 6 — Post-Mortem**: honest account of at least 2 things that went wrong, root
  cause, and what was learned. Minimum 1 full page.
- [ ] **Section 7 — References**: cite at minimum: Kleppmann DDIA Ch 10-12, ECML 2015 Porto
  paper, Flink documentation, Cassandra data modelling guide (all listed in §9.8 of brief)
- [ ] Report submitted as PDF to `report/taasim-technical-report.pdf`
- [ ] Both founders have reviewed and can defend every section in Q&A

## Technical Hints
- **Post-mortem template** (use this structure for each failure):
  ```
  Failure: [what broke]
  Sprint: [when it happened]
  Root Cause: [why it happened — technical explanation]
  Impact: [what it blocked or delayed]
  Resolution: [what was done to fix it]
  Lesson: [what would be done differently next time]
  ```
  Honest examples of failures worth documenting: Spark OOM on NYC TLC without proper
  partitioning, watermark misconfiguration causing late events to be silently dropped,
  Cassandra schema hotspot discovered under load, ML model overfitting before temporal split.

- **ADR-002 Kappa vs Lambda** key argument: Flink handles both real-time and historical replay
  (via Kafka topic retention = 7 days). Adding a separate Spark Streaming layer would duplicate
  processing logic, increase operational complexity, and add no precision to the real-time pipeline.
  Spark's role is batch ETL and ML training — tasks for which it is clearly superior to Flink.

- **ADR-003 GBT reasoning**: ARIMA requires stationarity and does not handle multiple input
  features. Prophet is univariate. LSTM requires more data and GPU resources than available.
  GBT handles mixed feature types (categorical + numerical + boolean) natively, is interpretable
  via feature importance, and trains efficiently on PySpark MLlib with the available dataset size.

- Recommended structure for Section 1 architecture narrative: describe data flow as a journey
  ("a GPS ping published by taxi 237 at 08:14 am travels through...") — this is more readable
  than a dry component list and demonstrates deep understanding.

- Reference: project brief §8 Evaluation Rubric (Technical Report pillar — both Passing and
  Distinction criteria), §9.8 Recommended Reading.

## Assigned To
Founder A + Founder B (joint)

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
