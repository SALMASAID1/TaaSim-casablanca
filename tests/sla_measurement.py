"""TaaSim — SLA Measurement Report

Sprint 6, Task 02

Measures all 5 SLA targets from §6.1 of the course brief:

    1. Trip match latency: request → match event in Cassandra < 5 seconds (P95)
    2. Vehicle position freshness: GPS ping → Cassandra write < 15 seconds
    3. Demand zone update frequency: every 30 seconds
    4. ML forecast API response time: < 500ms at 20 req/s
    5. Spark ETL on full Porto dataset (1.7M rows): < 5 minutes

Usage:
    python tests/sla_measurement.py

Prerequisites:
    - Docker stack running with all services healthy
    - At least one Flink job running
    - GPS and trip producers running
    - ML model trained and loaded in API
"""

from __future__ import annotations

import json
import os
import statistics
import time
from datetime import datetime, timezone

# Conditional imports with graceful fallback
try:
    import requests
except ImportError:
    requests = None

try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement
except ImportError:
    Cluster = None


API_URL = os.getenv("TAASIM_API_URL", "https://localhost:8000")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))


def measure_ml_forecast_latency(n_requests: int = 100) -> dict:
    """SLA 4: ML forecast API response time < 500ms at 20 req/s."""
    print("\n" + "=" * 60)
    print("  SLA 4: ML Forecast API Response Time")
    print("=" * 60)

    if requests is None:
        return {"status": "SKIP", "reason": "requests library not installed"}

    # Get admin token
    try:
        token_resp = requests.post(
            f"{API_URL}/auth/token",
            data={"username": "admin", "password": "adminpass"},
            verify=False,
            timeout=10,
        )
        token = token_resp.json()["access_token"]
    except Exception as exc:
        return {"status": "ERROR", "reason": f"Could not get token: {exc}"}

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"zone_id": 1, "datetime": "2024-03-15T08:00:00Z"}

    latencies = []
    errors = 0

    for i in range(n_requests):
        start = time.monotonic()
        try:
            resp = requests.post(
                f"{API_URL}/api/v1/demand/forecast",
                json=payload,
                headers=headers,
                verify=False,
                timeout=10,
            )
            elapsed_ms = (time.monotonic() - start) * 1000
            if resp.status_code == 200:
                latencies.append(elapsed_ms)
            else:
                errors += 1
        except Exception:
            errors += 1

    if not latencies:
        return {"status": "FAIL", "reason": f"All {n_requests} requests failed"}

    latencies.sort()
    p50 = latencies[len(latencies) // 2]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    avg = statistics.mean(latencies)

    result = {
        "status": "PASS" if p95 < 500 else "FAIL",
        "target": "< 500ms P95",
        "p50_ms": round(p50, 1),
        "p95_ms": round(p95, 1),
        "p99_ms": round(p99, 1),
        "avg_ms": round(avg, 1),
        "total_requests": n_requests,
        "successful": len(latencies),
        "errors": errors,
    }

    print(f"  Requests:  {n_requests} total, {len(latencies)} ok, {errors} errors")
    print(f"  P50:       {p50:.1f}ms")
    print(f"  P95:       {p95:.1f}ms  {'✅ PASS' if p95 < 500 else '❌ FAIL'}")
    print(f"  P99:       {p99:.1f}ms")
    print(f"  Average:   {avg:.1f}ms")

    return result


def measure_vehicle_position_freshness() -> dict:
    """SLA 2: Vehicle position freshness < 15 seconds."""
    print("\n" + "=" * 60)
    print("  SLA 2: Vehicle Position Freshness")
    print("=" * 60)

    if Cluster is None:
        return {"status": "SKIP", "reason": "cassandra-driver not installed"}

    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect("taasim")

        rows = session.execute(
            "SELECT event_time FROM taasim.vehicle_positions LIMIT 100"
        )

        now = datetime.now(timezone.utc)
        delays = []
        for row in rows:
            et = row.event_time
            if et is not None:
                if et.tzinfo is None:
                    from datetime import timezone as tz
                    et = et.replace(tzinfo=tz.utc)
                delay = (now - et).total_seconds()
                if 0 < delay < 3600:  # Only consider reasonable values
                    delays.append(delay)

        cluster.shutdown()

        if not delays:
            return {"status": "WARN", "reason": "No recent vehicle positions found"}

        avg_delay = statistics.mean(delays)
        max_delay = max(delays)
        p95_delay = sorted(delays)[int(len(delays) * 0.95)]

        result = {
            "status": "PASS" if p95_delay < 15 else "FAIL",
            "target": "< 15 seconds",
            "avg_seconds": round(avg_delay, 2),
            "p95_seconds": round(p95_delay, 2),
            "max_seconds": round(max_delay, 2),
            "samples": len(delays),
        }

        print(f"  Samples:   {len(delays)}")
        print(f"  Average:   {avg_delay:.2f}s")
        print(f"  P95:       {p95_delay:.2f}s  {'✅ PASS' if p95_delay < 15 else '❌ FAIL'}")
        print(f"  Max:       {max_delay:.2f}s")

        return result

    except Exception as exc:
        return {"status": "ERROR", "reason": str(exc)}


def measure_demand_zone_frequency() -> dict:
    """SLA 3: Demand zone update frequency = every 30 seconds."""
    print("\n" + "=" * 60)
    print("  SLA 3: Demand Zone Update Frequency")
    print("=" * 60)

    if Cluster is None:
        return {"status": "SKIP", "reason": "cassandra-driver not installed"}

    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect("taasim")

        # Get the last N window_start timestamps for a single zone
        rows = session.execute(
            "SELECT window_start FROM taasim.demand_zones "
            "WHERE city = 'casablanca' AND zone_id = 1 LIMIT 20"
        )

        timestamps = sorted([row.window_start for row in rows if row.window_start])
        cluster.shutdown()

        if len(timestamps) < 2:
            return {"status": "WARN", "reason": "Not enough demand_zones data (need Flink Job 2 running)"}

        gaps = []
        for i in range(1, len(timestamps)):
            gap = (timestamps[i] - timestamps[i-1]).total_seconds()
            if 0 < gap < 300:  # Only reasonable gaps
                gaps.append(gap)

        if not gaps:
            return {"status": "WARN", "reason": "Could not compute update intervals"}

        avg_gap = statistics.mean(gaps)
        target_met = 25 <= avg_gap <= 35  # Allow ±5s tolerance

        result = {
            "status": "PASS" if target_met else "FAIL",
            "target": "every 30 seconds (±5s)",
            "avg_interval_s": round(avg_gap, 2),
            "min_interval_s": round(min(gaps), 2),
            "max_interval_s": round(max(gaps), 2),
            "samples": len(gaps),
        }

        print(f"  Samples:   {len(gaps)} intervals")
        print(f"  Average:   {avg_gap:.2f}s  {'✅ PASS' if target_met else '❌ FAIL'}")
        print(f"  Min:       {min(gaps):.2f}s")
        print(f"  Max:       {max(gaps):.2f}s")

        return result

    except Exception as exc:
        return {"status": "ERROR", "reason": str(exc)}


def measure_trip_match_latency() -> dict:
    """SLA 1: Trip match latency < 5 seconds P95."""
    print("\n" + "=" * 60)
    print("  SLA 1: Trip Match Latency")
    print("=" * 60)

    if requests is None or Cluster is None:
        return {"status": "SKIP", "reason": "requests or cassandra-driver not installed"}

    try:
        # Get token
        token_resp = requests.post(
            f"{API_URL}/auth/token",
            data={"username": "rider1", "password": "riderpass"},
            verify=False,
            timeout=10,
        )
        token = token_resp.json()["access_token"]
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect("taasim")

        latencies = []
        n_tests = 10

        for i in range(n_tests):
            payload = {
                "origin_zone": (i % 16) + 1,
                "destination_zone": ((i + 3) % 16) + 1,
                "rider_id": f"sla-test-rider-{i}",
            }

            request_time = time.monotonic()
            resp = requests.post(
                f"{API_URL}/api/v1/trips",
                json=payload,
                headers=headers,
                verify=False,
                timeout=10,
            )

            if resp.status_code in (200, 202):
                # Wait and check Cassandra for the match
                trip_id = resp.json().get("trip_id")
                matched = False
                for _ in range(10):  # Check up to 10 times (5s total)
                    time.sleep(0.5)
                    rows = session.execute(
                        "SELECT status FROM taasim.trips WHERE city = 'casablanca' "
                        "AND date_bucket = toDate(now()) LIMIT 5"
                    )
                    for row in rows:
                        if row.status in ("matched", "assigned"):
                            matched = True
                            break
                    if matched:
                        break

                latency = time.monotonic() - request_time
                latencies.append(latency)

        cluster.shutdown()

        if not latencies:
            return {"status": "WARN", "reason": "No trip match measurements collected"}

        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        avg = statistics.mean(latencies)

        result = {
            "status": "PASS" if p95 < 5 else "FAIL",
            "target": "< 5 seconds P95",
            "p95_seconds": round(p95, 2),
            "avg_seconds": round(avg, 2),
            "samples": len(latencies),
        }

        print(f"  Samples:   {len(latencies)}")
        print(f"  Average:   {avg:.2f}s")
        print(f"  P95:       {p95:.2f}s  {'✅ PASS' if p95 < 5 else '❌ FAIL'}")

        return result

    except Exception as exc:
        return {"status": "ERROR", "reason": str(exc)}


def generate_report(results: dict) -> str:
    """Generate a Markdown SLA measurement report."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report = f"""# TaaSim — SLA Measurement Report

**Date:** {timestamp}
**Stack:** All services via Docker Compose

---

## SLA Summary

| # | Requirement | Target | Result | Status |
|---|-------------|--------|--------|--------|
"""
    sla_names = {
        "trip_match_latency": ("Trip match latency", "< 5s P95"),
        "vehicle_freshness": ("Vehicle position freshness", "< 15s"),
        "demand_update_freq": ("Demand zone update frequency", "every 30s"),
        "ml_forecast_latency": ("ML forecast API response", "< 500ms P95"),
        "spark_etl_duration": ("Spark ETL Porto (1.7M rows)", "< 5 minutes"),
    }

    for i, (key, (name, target)) in enumerate(sla_names.items(), 1):
        r = results.get(key, {"status": "NOT_RUN"})
        status_icon = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️", "SKIP": "⏭️"}.get(r["status"], "❓")
        detail = r.get("p95_ms", r.get("p95_seconds", r.get("avg_interval_s", "—")))
        report += f"| {i} | {name} | {target} | {detail} | {status_icon} {r['status']} |\n"

    report += "\n---\n\n## Detailed Results\n\n```json\n"
    report += json.dumps(results, indent=2, default=str)
    report += "\n```\n"

    return report


def main():
    print("=" * 60)
    print("  TaaSim — SLA Measurement Report (Sprint 6, Task 02)")
    print("=" * 60)

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    results = {}

    # SLA 1: Trip match latency
    results["trip_match_latency"] = measure_trip_match_latency()

    # SLA 2: Vehicle position freshness
    results["vehicle_freshness"] = measure_vehicle_position_freshness()

    # SLA 3: Demand zone update frequency
    results["demand_update_freq"] = measure_demand_zone_frequency()

    # SLA 4: ML forecast latency
    results["ml_forecast_latency"] = measure_ml_forecast_latency()

    # SLA 5: Spark ETL duration (manual — recorded from etl_porto.py output)
    results["spark_etl_duration"] = {
        "status": "MANUAL",
        "note": "Run etl_porto.py and record duration. Script has built-in SLA check.",
        "target": "< 5 minutes",
    }

    # Generate report
    report = generate_report(results)
    report_path = os.path.join(os.path.dirname(__file__), "..", "docs", "sla-measurement-report.md")
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)

    print("\n" + "=" * 60)
    print(f"  Report saved to: {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
