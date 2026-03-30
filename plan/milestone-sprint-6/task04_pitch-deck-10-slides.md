# task04 — 10-Slide Investor Pitch Deck

## Context
Week 8 is framed as a seed-round investor pitch. The evaluation rubric awards 20% weight to the
Startup Pitch + Demo pillar, with Distinction requiring the pitch to be "compelling to a
non-technical observer" and the team to "defend tradeoffs under pressure." The pitch deck must
tell a coherent story from Casablanca's mobility problem to TaaSim's solution, architecture,
live demo, and business model — in exactly 10 slides delivered in 20 minutes, followed by 10
minutes of Q&A.

## Objective
Prepare a 10-slide investor pitch deck that covers all required topics, supports the live demo
segment, and is rehearsed to fit within 20 minutes.

## Acceptance Criteria
- [ ] Deck contains exactly 10 slides in the order specified below
- [ ] Slide 1 — **Problem**: Casablanca's mobility fragmentation pain points (data from §1 of brief)
  — visualised with the pain-point table, not just bullets
- [ ] Slide 2 — **Solution**: TaaSim value proposition — one sentence, one diagram
- [ ] Slide 3 — **Architecture**: simplified system diagram (Kafka → Flink → Cassandra →
  FastAPI/Grafana; Spark side-channel for ML) — non-technical audience: no JARs, no configs
- [ ] Slide 4 — **Live Demo**: placeholder slide ("DEMO" full-screen) — cue to switch to Grafana
- [ ] Slide 5 — **Metrics**: actual SLA results from task02 (trip match latency, position
  freshness, ML forecast accuracy) — real numbers, not targets
- [ ] Slide 6 — **Business Model**: B2B SaaS — licence fee per city; data API for city planners;
  premium analytics tier for fleet operators
- [ ] Slide 7 — **ML Insight**: feature importance chart from task04 — explained in plain language
  ("Friday afternoons in Zone 3 are 3× more likely to spike — here's why")
- [ ] Slide 8 — **Resilience**: checkpoint recovery screenshot from task01 — "the platform never
  loses a GPS ping"
- [ ] Slide 9 — **Roadmap**: 3 milestones: (1) Casablanca pilot 6 months, (2) Rabat/Marrakesh
  expansion 12 months, (3) Open data API for municipal planning 18 months
- [ ] Slide 10 — **Team**: photo (or icon), name, role, one-line bio for each founder
- [ ] Full rehearsal timed: presentation fits in 20 minutes
- [ ] Deck exported as PDF to `pitch/taasim-pitch-deck.pdf` and committed to repo
- [ ] Both founders can answer Q&A on any technical slide without reading from notes

## Technical Hints
- Slide 3 architecture diagram should be hand-drawn clean or done in draw.io/Excalidraw —
  avoid pasting the ASCII architecture from the brief directly.
- Slide 5 metrics: use the actual measured values from `docs/sla-measurement-table.md`, not
  the targets. If a target was missed, be honest — investors respect candour.
- Slide 6 business model: calculate a rough unit economics example:
  "City of Casablanca: 500 000 daily trips × 0.50 MAD data fee = 250 000 MAD/day data revenue".
  Even if rough, it shows product thinking.
- Slide 7 ML insight: translate `hour_of_day` being the top feature into:
  "Peak hours are the dominant demand signal — our model learns the city's rhythm."
- Prepare three likely Q&A questions and rehearse answers:
  1. "What happens when you scale to 10 cities?" → Kafka partitioning, Cassandra multi-DC
  2. "Why not just use Uber's API?" → Data sovereignty, local pricing, municipal integration
  3. "How accurate is your ML model?" → cite RMSE vs baseline, explain what it means for drivers
- Reference: project brief §8 Evaluation Rubric (Startup Pitch + Demo pillar), §7 W8 tasks.

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
