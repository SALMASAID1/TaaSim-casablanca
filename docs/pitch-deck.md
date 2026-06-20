# TaaSim — 10-Slide Investor Pitch Deck

**Transport as a Service · Urban Mobility Platform · Casablanca**

---

## Slide 1: Title

### TaaSim
**The Brain Behind Casablanca's Mobility Transformation**

*Advanced Big Data · ENSA Al Hoceima · 2025–2026*

Team: TaaSim Founders
- Mohamed Tamzirt, Big Data Engineering
- Salma Said, ML & Infrastructure

---

## Slide 2: The Problem

### 🚕 Casablanca's Mobility Crisis

- **4 million citizens** — Morocco's economic capital
- **Zero digital infrastructure** for taxis: no GPS tracking, no booking apps, no data
- Riders wait 15+ minutes for a taxi that may never come
- City planners have **no visibility** into demand patterns
- During peak hours, demand can spike **3× in specific zones**

> **Result:** Lost productivity, pollution from empty cruising, and zero ability to plan for population growth.

---

## Slide 3: The Solution

### TaaSim: Real-Time Urban Mobility Intelligence

A **Big Data platform** that connects riders to vehicles dynamically and gives city planners predictive visibility:

| Capability | How |
|-----------|-----|
| **Instant matching** | Real-time Flink streaming (< 5 second match) |
| **Demand forecasting** | ML model predicting demand 30 minutes ahead |
| **Live monitoring** | Grafana dashboards for 16 arrondissements |
| **Secure API** | JWT-protected REST endpoints for mobile apps |

---

## Slide 4: How It Works (Architecture)

### Kappa Architecture — Stream-First Design

```
  RIDERS      TAXIS         CITY PLANNERS
    │           │                │
    ▼           ▼                │
  FastAPI    GPS Kafka         Grafana
    │           │                ▲
    ▼           ▼                │
  ┌─────────────────────────────┤
  │     Apache Kafka (Events)   │
  │         │      │            │
  │     Flink 3  Spark ETL     │
  │      Jobs      + ML        │
  │         │      │           │
  │    Cassandra  MinIO        │
  │      (serve)  (store)      │
  └─────────────────────────────┘
```

**Key insight:** Single streaming pipeline (Kappa) reduces complexity by 50% versus Lambda Architecture.

---

## Slide 5: Live Demo

### 📺 Watch It Run

**Demo Flow (90 seconds):**

1. Start GPS producer → vehicle dots appear on Casablanca map (< 10s)
2. Submit trip request via API → matched to nearest vehicle (< 5s)
3. Inject demand spike (Zone 5, 3×) → heatmap lights up (< 60s)
4. Call ML forecast endpoint → predicted demand in < 500ms

**Grafana:** http://localhost:3000 (live during demo)

---

## Slide 6: Technology Deep Dive

### Production-Grade Stack

| Component | Technology | Why This Choice |
|-----------|-----------|-----------------|
| Events | Kafka (KRaft) | No Zookeeper, SASL/PLAIN security |
| Streaming | Flink 1.18 | Event-time semantics, checkpointing |
| Analytics | Spark 3.5 | PySpark ML + SQL for KPIs |
| Database | Cassandra 4.1 | < 2ms zone queries, partition-aligned |
| Storage | MinIO (S3) | Data lake with lifecycle zones |
| Dashboard | Grafana 10.4 | Cassandra plugin, auto-refresh |

---

## Slide 7: ML Results

### Demand Forecasting — GBT vs Naive Baseline

**Model:** Gradient-Boosted Trees (Spark MLlib)
**Features:** Temporal (hour, day), spatial (zone type), weather (rain), lag (7-day)

| Metric | Naive Baseline | TaaSim GBT | Improvement |
|--------|---------------|------------|-------------|
| RMSE | 20.40 | 14.46 | **29.1%** better |
| MAE | 15.20 | 10.75 | **29.3%** better |

> **Top predictor:** `demand_lag_7d` — explaining why certain zones spike at certain hours.

---

## Slide 8: Performance & Security

### Meeting All SLA Targets

| Metric | Target | Achieved |
|--------|--------|----------|
| Trip match latency | < 5s P95 | ✅ 0.42s |
| Position freshness | < 15s | ✅ 1.2s |
| Demand updates | every 30s | ✅ 30s |
| API forecast | < 500ms | ✅ 35ms |
| Spark ETL (1.7M rows) | < 5 min | ✅ 4:24 |

**Security:** JWT auth + Kafka ACLs + HTTPS (self-signed)

---

## Slide 9: Business Potential

### Path to Market

**Phase 1 — Pilot (0-6 months):**
- Partner with Casablanca transportation authority
- Deploy on 100 taxis (grand taxis, pilot arrondissement)
- Validate ML forecasts against real demand

**Phase 2 — Scale (6-18 months):**
- Full city deployment (10,000+ taxis)
- Mobile app for riders (React Native)
- Integration with ALSA bus network

**Phase 3 — Expand (18+ months):**
- Rabat, Marrakech, Tangier
- Multi-modal: buses, trams, shared bikes
- SaaS licensing to other African cities

**Revenue Model:** Per-ride commission (2-5%) + city planning analytics subscription

---

## Slide 10: Ask & Team

### What We Need

**Immediate:**
- Access to real GPS data from Casablanca taxi operators
- Azure/GCP credits for cloud deployment testing

**Long-Term:**
- Seed funding for mobile app development
- Partnership with Casa Transport authority

### The Team

| | Role | Background |
|---|------|-----------|
| Mohamed Tamzirt | Data Engineering | Kafka, Flink, Cassandra |
| Salma Said | ML & Infrastructure | Spark MLlib, Docker, MinIO |

**ENSA Al Hoceima · Advanced Big Data · Class of 2026**

---

> **Contact:** [email] · [GitHub repo URL]
>
> *"Bringing data-driven mobility to Morocco — one zone at a time."*
