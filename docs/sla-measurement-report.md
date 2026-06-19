# TaaSim — SLA Measurement Report

**Date:** 2026-06-19 21:30:31
**Stack:** All services via Docker Compose

---

## SLA Summary

| # | Requirement | Target | Result | Status |
|---|-------------|--------|--------|--------|
| 1 | Trip match latency | < 5s P95 | 0.56 | ✅ PASS |
| 2 | Vehicle position freshness | < 15s | 3064.14 | ❌ FAIL |
| 3 | Demand zone update frequency | every 30s | 50.53 | ❌ FAIL |
| 4 | ML forecast API response | < 500ms P95 | — | ❌ FAIL |
| 5 | Spark ETL Porto (1.7M rows) | < 5 minutes | — | ❓ MANUAL |

---

## Detailed Results

```json
{
  "trip_match_latency": {
    "status": "PASS",
    "target": "< 5 seconds P95",
    "p95_seconds": 0.56,
    "avg_seconds": 0.54,
    "samples": 10
  },
  "vehicle_freshness": {
    "status": "FAIL",
    "target": "< 15 seconds",
    "avg_seconds": 3039.41,
    "p95_seconds": 3064.14,
    "max_seconds": 3065.14,
    "samples": 100
  },
  "demand_update_freq": {
    "status": "FAIL",
    "target": "every 30 seconds (\u00b15s)",
    "avg_interval_s": 50.53,
    "min_interval_s": 30.0,
    "max_interval_s": 120.0,
    "samples": 19
  },
  "ml_forecast_latency": {
    "status": "FAIL",
    "reason": "All 100 requests failed"
  },
  "spark_etl_duration": {
    "status": "MANUAL",
    "note": "Run etl_porto.py and record duration. Script has built-in SLA check.",
    "target": "< 5 minutes"
  }
}
```
