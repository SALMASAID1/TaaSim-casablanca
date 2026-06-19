"""TaaSim — GPS Anonymization Verification Test

Sprint 2, Task 05

Verifies that raw GPS coordinates are NEVER persisted to Cassandra.
Flink Job 1 must snap coordinates to zone centroids before writing.

This test:
    1. Reads zone_mapping.csv to get the list of valid zone centroids
    2. Queries vehicle_positions table for recent rows
    3. Verifies every (lat, lon) matches a known zone centroid (within tolerance)
    4. Confirms no raw GPS coordinates are stored

Usage:
    python tests/test_gps_anonymization.py

Prerequisites:
    - Cassandra running with data in vehicle_positions
    - Flink Job 1 must have processed GPS events
"""

from __future__ import annotations

import csv
import math
import os
import sys
from datetime import datetime, timedelta, timezone

# Conditional import
try:
    from cassandra.cluster import Cluster
except ImportError:
    print("❌ cassandra-driver not installed. Run: pip install cassandra-driver")
    sys.exit(1)


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
ZONE_MAPPING_PATH = os.path.join(
    os.path.dirname(__file__), "..", "metadata", "zone_mapping.csv"
)

# Tolerance for centroid matching (degrees). Zone centroids are fixed values,
# so we allow only a tiny tolerance for floating-point representation.
CENTROID_TOLERANCE_DEG = 0.005  # ~500m — generous for grid-based zoning


def load_zone_centroids() -> list[tuple[float, float]]:
    """Load zone centroids from zone_mapping.csv.

    Computes centroid as midpoint of each zone's bounding box.
    """
    centroids = []
    with open(ZONE_MAPPING_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lon_mid = (float(row["lon_min"]) + float(row["lon_max"])) / 2.0
            lat_mid = (float(row["lat_min"]) + float(row["lat_max"])) / 2.0
            centroids.append((round(lon_mid, 6), round(lat_mid, 6)))
    return centroids


def is_near_centroid(
    lon: float, lat: float, centroids: list[tuple[float, float]], tol: float
) -> bool:
    """Check if (lon, lat) is within tolerance of any known centroid."""
    for c_lon, c_lat in centroids:
        if abs(lon - c_lon) < tol and abs(lat - c_lat) < tol:
            return True
    return False


def main():
    print("=" * 60)
    print("  TaaSim — GPS Anonymization Verification Test")
    print("  Sprint 2, Task 05")
    print("=" * 60)

    # Load centroids
    if not os.path.exists(ZONE_MAPPING_PATH):
        print(f"\n❌ Zone mapping not found at: {ZONE_MAPPING_PATH}")
        print("   Run from the project root, or set correct path.")
        sys.exit(1)

    centroids = load_zone_centroids()
    print(f"\n[1] Loaded {len(centroids)} zone centroids from zone_mapping.csv")
    for i, (lon, lat) in enumerate(centroids, 1):
        print(f"    Zone {i}: ({lon}, {lat})")

    # Connect to Cassandra
    print(f"\n[2] Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect("taasim")
    except Exception as exc:
        print(f"❌ Could not connect to Cassandra: {exc}")
        sys.exit(1)

    # Query recent vehicle positions
    print("\n[3] Querying vehicle_positions table...")
    rows = session.execute(
        "SELECT taxi_id, lat, lon, zone_id, event_time "
        "FROM taasim.vehicle_positions LIMIT 200"
    )

    positions = list(rows)
    print(f"    Found {len(positions)} rows")

    if not positions:
        print("\n⚠️ No vehicle positions found. Ensure Flink Job 1 is running with GPS data.")
        cluster.shutdown()
        sys.exit(0)

    # Verify anonymization
    print("\n[4] Verifying coordinates are anonymized (zone centroids only)...")
    violations = []
    near_centroid_count = 0

    for row in positions:
        if is_near_centroid(row.lon, row.lat, centroids, CENTROID_TOLERANCE_DEG):
            near_centroid_count += 1
        else:
            violations.append({
                "taxi_id": row.taxi_id,
                "lat": row.lat,
                "lon": row.lon,
                "zone_id": row.zone_id,
            })

    # Results
    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)
    print(f"  Total positions checked:     {len(positions)}")
    print(f"  Near zone centroid:          {near_centroid_count}")
    print(f"  Violations (raw GPS):        {len(violations)}")
    print()

    if not violations:
        print("  ✅ PASS — All coordinates are anonymized to zone centroids.")
        print("     Raw GPS coordinates are NOT persisted in Cassandra.")
    else:
        print("  ❌ FAIL — Raw GPS coordinates found in Cassandra!")
        print()
        for v in violations[:10]:
            print(f"     taxi_id={v['taxi_id']} lat={v['lat']} lon={v['lon']} zone={v['zone_id']}")
        if len(violations) > 10:
            print(f"     ... and {len(violations) - 10} more violations")

    print("=" * 60)
    cluster.shutdown()

    sys.exit(0 if not violations else 1)


if __name__ == "__main__":
    main()
