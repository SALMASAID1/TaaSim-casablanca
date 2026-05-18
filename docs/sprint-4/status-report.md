# TaaSim · Casablanca — Sprint 4 Status Sync Report

**Report Date:** 2026-05-05 · 11:15 (Africa/Casablanca)  
**Prepared for:** AI Advisor  
**Prepared by:** Co-Founder · TaaSim Casablanca  

---

## 1. Current Milestone

| Dimension | Value |
|---|---|
| **Active Sprint** | **Sprint 4 — Batch Analytics Pipeline** |
| **Sprint Focus** | Spark ETL optimizations, Batch KPI computation, Grafana Analytics Panels |

### Sprint 4 — Batch Analytics Pipeline ✅ COMPLETE

All Sprint 4 tasks have been implemented and verified:

- [x] **Task 01: Spark ETL Porto Dataset** — Implemented `spark_jobs/etl_porto.py` with POLYLINE parsing, zone remapping via broadcast join, deduplication on `TRIP_ID`, and Parquet output partitioned by `year_month`. Job optimized to run under the 5-minute SLA.
- [x] **Task 02: Spark ETL NYC TLC Dataset** — Implemented `spark_jobs/etl_nyc_tlc.py`. Processes large-scale data (3 months, ~30M rows) to compute per-zone-per-hour demand aggregates (`trip_count`, `avg_distance`, `avg_fare`), optimizing Spark shuffles (`spark.sql.shuffle.partitions=16`).
- [x] **Task 03: Weekly KPI Computation** — Implemented `spark_jobs/kpi_weekly.py`. Fixed logical calculation errors in average trip duration (now dynamically computed from `gps_point_count` instead of hardcoded) and peak hours aggregation. Successfully computes 4 core KPIs: Trips per zone (Spark SQL), Avg trip duration, Peak demand hours (Spark SQL + Window functions), and Coverage gap. Writes results directly to Cassandra (`kpi_weekly`, `kpi_peak_hours`, `demand_zones`).
- [x] **Task 04: Grafana KPI Panels** — Provisioned the `taasim-live` dashboard with 4 fully functional panels displaying the Spark KPI results, querying Cassandra directly using the `hadesarchitect-cassandra-datasource`. Panel 4 has been properly restored as a Bar Chart displaying aggregated city-wide peak hours.

---

## 2. Infrastructure Health & Artifacts

### Cassandra Tables (Sprint 4 Updates)
Cassandra schema was expanded via `db/cassandra_init.cql` to support the new analytics queries:

| Table | Purpose | Status |
|---|---|---|
| `kpi_weekly` | Stores Trips/Zone, Avg Duration, and Coverage Gap metrics | ✅ Populated (3,106 rows) |
| `kpi_peak_hours` | Stores hourly trip counts for the bar chart visualization | ✅ Populated (20,347 rows) |
| `demand_zones` | Enriched with batch `forecast_demand` to overlay with real-time data | ✅ Populated (16 rows) |

### Grafana Dashboards
- **Datasource Provisioning**: Cassandra datasource auto-configured via `grafana/provisioning/datasources/datasources.yml`.
- **Dashboard Provisioning**: `taasim-live-v2` dashboard provisioned via JSON with correct `queryType: "query"` and `rawQuery: true` parameters to resolve Cassandra plugin limitations.

---

## 3. Pipeline Highlights & Technical Decisions

### 3.1 Spark ETL Optimizations
- **Data Reduction Before Parsing**: In `etl_porto.py`, we implemented data deduplication and filtering *before* parsing the complex `POLYLINE` field to minimize data volume early in the DAG.
- **Broadcast Joins**: Used `broadcast(zone_ref)` for joining the 16-row `zone_mapping.csv` reference table to avoid expensive shuffle operations in both Porto and NYC TLC ETL jobs.
- **Adaptive Query Execution (AQE)**: Enabled Spark AQE for dynamic coalescing of shuffle partitions, keeping single-worker execution extremely fast.

**2026-05-05 update — Data quality hardening**
- **ADR-01 Affine Mapping (Porto → Casablanca)**: `etl_porto.py` now uses the same *relative-position bbox affine transform* logic as Notebook 03 (instead of a constant lat/lon shift), producing more consistent spatial structure and zone coverage.
- **Origin + Destination Capture (No Explode)**: `etl_porto.py` now extracts both first/last GPS points per trip (origin/destination) without exploding all POLYLINE points, keeping runtime/SLA stable.
- **No Random Zone Fallback**: Hash-based “Zone-Assigned” fallback is removed. Unmatched points are explicitly tagged as `out_of_bounds` (zone_id=0), and the ETL fails if the out_of_bounds rate exceeds a threshold.
- **NYC Audit Gate**: `etl_nyc_tlc.py` now validates the 16-row zone reference table and fails if the aggregated output contains NULL zone fields.

### 3.2 Cassandra & Grafana Integration
- **Schema Alignment**: Addressed schema mismatches where group-by operations accidentally dropped partition keys (e.g., `week_start`).
- **Resiliency**: Implemented a Parquet fallback mechanism (`s3a://taasim/curated/kpi/...`) in `kpi_weekly.py` to ensure analytics results are persisted even if the Cassandra cluster is temporarily unavailable.
- **Grafana Panel Tuning**: Converted the "Peak Hours" visualization back to a native Bar Chart. Bypassed the Cassandra plugin limitation by aggregating a city-wide total (`zone_id=0`) directly within the Spark job, allowing Grafana to render the chart efficiently without complex client-side transforms.

---

## 4. Next Steps (Sprint 5)

With the Batch Analytics Pipeline complete, the platform is ready for the ML Demand Forecasting sprint.

| Priority | Task | Sprint | Impact |
|---|---|---|---|
| 🔴 **P0** | Implement JWT Authentication for API | Sprint 5 | Security foundation for public endpoints |
| 🔴 **P0** | Feature Engineering Pipeline | Sprint 5 | Prepares spatial/temporal/lag features for the ML model |
| 🟡 **P1** | Train GBT Model & Validation | Sprint 5 | Core ML deliverable to beat the naive 7-day baseline |
| 🟡 **P1** | FastAPI Forecast Endpoint | Sprint 5 | Exposes the ML model for low-latency serving |

---

## Summary Dashboard

```
┌─────────────────────────────────────────────────────────────────────┐
│  TaaSim · Casablanca — Status at 2026-05-05 11:15                  │
│                                                                     │
│  Milestone    ████████████████░░░░  Sprint 4 (Batch Analytics) ✅  │
│  Spark ETL    ████████████████████  Porto & NYC Jobs Complete      │
│  KPI Jobs     ████████████████████  All 4 KPIs computing           │
│  Dashboards   ████████████████████  4 Panels Live in Grafana       │
│                                                                     │
│  NEXT SPRINT → Sprint 5: Security, ML Pipeline & Forecast API      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

*Report generated by automated workspace introspection against live Docker stack, Cassandra cluster, and local filesystem.*
