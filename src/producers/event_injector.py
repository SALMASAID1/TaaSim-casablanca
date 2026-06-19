"""TaaSim — Event Injector (Demo Day Anomaly Generator)

Sprint 6, Task 03

Injects configurable anomalies into the TaaSim streaming pipeline for live demo:

    1. **Demand Spike** — Floods a chosen zone with 3× trip requests for N minutes.
       Simulates a stadium exit, train cancellation, or concert ending.

    2. **GPS Blackout** — Suppresses all GPS events from a configurable set of vehicles
       for a configurable duration. Simulates a network outage or tunnel.

    3. **Rain Event** — Increases the global trip request rate by 1.4× for a
       configurable period. Simulates weather-driven demand surge.

Usage
-----
    # Demand spike in zone 5 for 3 minutes at 3× intensity
    python event_injector.py spike --zone 5 --duration 180 --multiplier 3.0

    # GPS blackout for 10 vehicles for 2 minutes
    python event_injector.py blackout --num-vehicles 10 --duration 120

    # Rain event globally for 5 minutes at 1.4× demand multiplier
    python event_injector.py rain --duration 300 --multiplier 1.4

Configuration
-------------
    TAASIM_KAFKA_BROKER     (default: localhost:9092)
    TAASIM_SPEED            (default: 10)
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import signal
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("event_injector")


# ---------------------------------------------------------------------------
# Constants — Casablanca zone centroids (from zone_mapping.csv)
# ---------------------------------------------------------------------------
ZONE_CENTROIDS: Dict[int, tuple] = {
    1:  (-7.605, 33.577),
    2:  (-7.555, 33.577),
    3:  (-7.655, 33.577),
    4:  (-7.705, 33.577),
    5:  (-7.505, 33.577),
    6:  (-7.605, 33.535),
    7:  (-7.555, 33.535),
    8:  (-7.655, 33.535),
    9:  (-7.705, 33.535),
    10: (-7.505, 33.535),
    11: (-7.605, 33.620),
    12: (-7.555, 33.620),
    13: (-7.655, 33.620),
    14: (-7.705, 33.620),
    15: (-7.505, 33.620),
    16: (-7.605, 33.600),
}

NUM_ZONES = 16

# ---------------------------------------------------------------------------
# Kafka helper
# ---------------------------------------------------------------------------

def _create_producer(broker: str) -> KafkaProducer:
    """Create a Kafka producer with SASL/PLAIN auth matching the project config."""
    return KafkaProducer(
        bootstrap_servers=[broker],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks=1,
        retries=5,
        linger_ms=10,
        compression_type="gzip",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="trip-producer",
        sasl_plain_password="trip-producer-secret",
    )


def _iso_now() -> str:
    """Current UTC time as ISO-8601 with trailing Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Anomaly 1: Demand Spike
# ---------------------------------------------------------------------------

def inject_demand_spike(
    broker: str,
    zone_id: int,
    duration_s: float,
    multiplier: float,
    base_rate: float = 2.0,
    tick_s: float = 0.5,
) -> None:
    """Flood a single zone with elevated trip request rate.

    Parameters
    ----------
    zone_id : int
        Target Casablanca zone (1–16).
    duration_s : float
        How long the spike lasts (seconds of wall-clock time).
    multiplier : float
        Demand multiplier relative to base rate (e.g. 3.0 = 3× normal).
    base_rate : float
        Normal trip requests per second at peak hour.
    tick_s : float
        Emission tick interval.
    """
    logger.info(
        "💥 DEMAND SPIKE — zone=%d, duration=%.0fs, multiplier=%.1fx, base_rate=%.1f ev/s",
        zone_id, duration_s, multiplier, base_rate,
    )

    producer = _create_producer(broker)
    topic = "raw.trips"
    produced = 0
    start = time.monotonic()

    try:
        while time.monotonic() - start < duration_s:
            # Events per tick = rate × multiplier × tick_s
            expected = base_rate * multiplier * tick_s
            n_events = max(0, int(random.gauss(expected, max(1, expected ** 0.5))))

            for _ in range(n_events):
                trip_id = str(uuid.uuid4())
                dest_zone = random.randint(1, NUM_ZONES)
                while dest_zone == zone_id:
                    dest_zone = random.randint(1, NUM_ZONES)

                event = {
                    "trip_id": trip_id,
                    "rider_id": f"rider-spike-{random.randint(1, 500)}",
                    "origin_zone": zone_id,
                    "destination_zone": dest_zone,
                    "requested_at": _iso_now(),
                    "call_type": random.choice(["A", "B", "C"]),
                    "injected": True,
                    "anomaly_type": "demand_spike",
                }
                producer.send(topic, key=trip_id, value=event)
                produced += 1

            time.sleep(tick_s)

            elapsed = time.monotonic() - start
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                logger.info(
                    "  Spike progress: %.0f/%.0fs — %d events produced",
                    elapsed, duration_s, produced,
                )

    except KeyboardInterrupt:
        logger.info("Spike interrupted by user.")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=5)

    logger.info("💥 DEMAND SPIKE complete — %d events injected into zone %d", produced, zone_id)


# ---------------------------------------------------------------------------
# Anomaly 2: GPS Blackout
# ---------------------------------------------------------------------------

def inject_gps_blackout(
    broker: str,
    num_vehicles: int,
    duration_s: float,
) -> None:
    """Suppress GPS events by flooding blackout markers for N vehicles.

    In the real pipeline, the GPS producer handles blackouts via delayed sends.
    This injector simulates a *different* scenario: we send explicit 'blackout'
    status events so Flink Job 1 sees vehicles go offline.

    Parameters
    ----------
    num_vehicles : int
        Number of vehicles to black out.
    duration_s : float
        Blackout duration (wall-clock seconds).
    """
    logger.info(
        "🔇 GPS BLACKOUT — %d vehicles for %.0fs",
        num_vehicles, duration_s,
    )

    # Create a GPS-producer-scoped Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks=1,
        retries=5,
        linger_ms=10,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="gps-producer",
        sasl_plain_password="gps-producer-secret",
    )

    topic = "raw.gps"
    taxi_ids = [f"blackout-taxi-{i:04d}" for i in range(num_vehicles)]
    produced = 0
    start = time.monotonic()

    try:
        # Send initial "going offline" marker for each vehicle
        for taxi_id in taxi_ids:
            zone = random.randint(1, NUM_ZONES)
            centroid = ZONE_CENTROIDS.get(zone, (-7.605, 33.577))
            event = {
                "taxi_id": taxi_id,
                "timestamp": _iso_now(),
                "lat": centroid[1],
                "lon": centroid[0],
                "speed": 0.0,
                "status": "offline",
                "injected": True,
                "anomaly_type": "gps_blackout",
            }
            producer.send(topic, key=taxi_id, value=event)
            produced += 1

        logger.info("  Sent %d offline markers. Waiting %.0fs for blackout duration...", num_vehicles, duration_s)

        # Wait for the blackout period (no GPS events sent)
        remaining = duration_s
        while remaining > 0:
            sleep_time = min(remaining, 10.0)
            time.sleep(sleep_time)
            remaining -= sleep_time
            logger.info("  Blackout: %.0fs remaining...", remaining)

        # Send "coming back online" for each vehicle
        for taxi_id in taxi_ids:
            zone = random.randint(1, NUM_ZONES)
            centroid = ZONE_CENTROIDS.get(zone, (-7.605, 33.577))
            event = {
                "taxi_id": taxi_id,
                "timestamp": _iso_now(),
                "lat": centroid[1] + random.gauss(0, 0.001),
                "lon": centroid[0] + random.gauss(0, 0.001),
                "speed": round(random.uniform(5, 40), 2),
                "status": "available",
                "injected": True,
                "anomaly_type": "gps_blackout_recovery",
            }
            producer.send(topic, key=taxi_id, value=event)
            produced += 1

    except KeyboardInterrupt:
        logger.info("Blackout interrupted by user.")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=5)

    logger.info("🔇 GPS BLACKOUT complete — %d total events for %d vehicles", produced, num_vehicles)


# ---------------------------------------------------------------------------
# Anomaly 3: Rain Event
# ---------------------------------------------------------------------------

def inject_rain_event(
    broker: str,
    duration_s: float,
    multiplier: float = 1.4,
    base_rate: float = 1.5,
    tick_s: float = 0.5,
) -> None:
    """Increase global trip request rate to simulate rain-driven demand surge.

    Parameters
    ----------
    duration_s : float
        Rain event duration (wall-clock seconds).
    multiplier : float
        Demand multiplier (default 1.4× per course spec).
    base_rate : float
        Normal trip requests per second.
    tick_s : float
        Emission tick interval.
    """
    logger.info(
        "🌧️ RAIN EVENT — duration=%.0fs, multiplier=%.1fx, base_rate=%.1f ev/s",
        duration_s, multiplier, base_rate,
    )

    producer = _create_producer(broker)
    topic = "raw.trips"
    produced = 0
    start = time.monotonic()

    try:
        while time.monotonic() - start < duration_s:
            expected = base_rate * multiplier * tick_s
            n_events = max(0, int(random.gauss(expected, max(1, expected ** 0.5))))

            for _ in range(n_events):
                trip_id = str(uuid.uuid4())
                origin = random.randint(1, NUM_ZONES)
                dest = random.randint(1, NUM_ZONES)
                while dest == origin:
                    dest = random.randint(1, NUM_ZONES)

                event = {
                    "trip_id": trip_id,
                    "rider_id": f"rider-rain-{random.randint(1, 2000)}",
                    "origin_zone": origin,
                    "destination_zone": dest,
                    "requested_at": _iso_now(),
                    "call_type": random.choice(["A", "B", "C"]),
                    "injected": True,
                    "anomaly_type": "rain_event",
                }
                producer.send(topic, key=trip_id, value=event)
                produced += 1

            time.sleep(tick_s)

            elapsed = time.monotonic() - start
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                logger.info(
                    "  Rain progress: %.0f/%.0fs — %d events produced",
                    elapsed, duration_s, produced,
                )

    except KeyboardInterrupt:
        logger.info("Rain event interrupted by user.")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=5)

    logger.info("🌧️ RAIN EVENT complete — %d events injected globally", produced)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TaaSim Event Injector — Demo Day anomaly generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python event_injector.py spike --zone 5 --duration 180 --multiplier 3.0
  python event_injector.py blackout --num-vehicles 10 --duration 120
  python event_injector.py rain --duration 300 --multiplier 1.4
        """,
    )

    parser.add_argument(
        "--broker",
        default=os.environ.get("TAASIM_KAFKA_BROKER", "localhost:9092"),
        help="Kafka bootstrap server (default: localhost:9092)",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # --- Demand Spike ---
    sp_spike = sub.add_parser("spike", help="Inject a demand spike in a specific zone")
    sp_spike.add_argument("--zone", type=int, required=True, help="Target zone ID (1-16)")
    sp_spike.add_argument("--duration", type=float, default=300, help="Duration in seconds (default: 300)")
    sp_spike.add_argument("--multiplier", type=float, default=3.0, help="Demand multiplier (default: 3.0)")
    sp_spike.add_argument("--base-rate", type=float, default=2.0, help="Base events per second (default: 2.0)")

    # --- GPS Blackout ---
    sp_black = sub.add_parser("blackout", help="Simulate a GPS blackout for N vehicles")
    sp_black.add_argument("--num-vehicles", type=int, default=10, help="Number of vehicles (default: 10)")
    sp_black.add_argument("--duration", type=float, default=120, help="Blackout duration in seconds (default: 120)")

    # --- Rain Event ---
    sp_rain = sub.add_parser("rain", help="Simulate a rain event (global demand increase)")
    sp_rain.add_argument("--duration", type=float, default=300, help="Duration in seconds (default: 300)")
    sp_rain.add_argument("--multiplier", type=float, default=1.4, help="Demand multiplier (default: 1.4)")
    sp_rain.add_argument("--base-rate", type=float, default=1.5, help="Base events per second (default: 1.5)")

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # Graceful shutdown
    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("Received signal %s — stopping...", signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if args.command == "spike":
        inject_demand_spike(
            broker=args.broker,
            zone_id=args.zone,
            duration_s=args.duration,
            multiplier=args.multiplier,
            base_rate=args.base_rate,
        )
    elif args.command == "blackout":
        inject_gps_blackout(
            broker=args.broker,
            num_vehicles=args.num_vehicles,
            duration_s=args.duration,
        )
    elif args.command == "rain":
        inject_rain_event(
            broker=args.broker,
            duration_s=args.duration,
            multiplier=args.multiplier,
            base_rate=args.base_rate,
        )


if __name__ == "__main__":
    main()
