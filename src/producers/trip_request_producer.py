"""TaaSim — Trip Request Producer

Emits *citizen reservation* events to Kafka topic `raw.trips`.

Why this exists
--------------
Casablanca does not have an open, real-time trip reservation feed. The course brief therefore
requires a simulation layer that generates `raw.trips` events following the *Porto demand curve*
(peaks at 7–9 and 17–19), with additional adjustments:

- Friday 12–14 reduced (jumu'ah pattern)
- Sunday reduced (weekly low)

The resulting stream is consumed by:
- Flink Job 2 (Demand Aggregator): joins reservations with vehicle positions
- Flink Job 3 (Trip Matcher): matches riders to available vehicles

Event schema (JSON)
-------------------
Each Kafka message value is JSON with fields:

- `trip_id` (UUID string)
- `rider_id` (string)
- `origin_zone` (int, 1..N)
- `destination_zone` (int, 1..N)
- `requested_at` (ISO-8601 string)
- `call_type` ("A" | "B" | "C")

Time model
----------
This producer uses *event time* generated from a simulated clock.
`--speed` accelerates simulated time relative to wall-clock time:

- If `speed = 10`, then 1 second wall-clock = 10 seconds simulated.

The emission rate is defined in events / simulated-second, so increasing `speed` increases
throughput proportionally (to keep the same event density per simulated time).

Configuration
-------------
All CLI arguments can also be provided via environment variables:

- `TAASIM_KAFKA_BROKER` (default: localhost:9092)
- `TAASIM_TRIPS_TOPIC` (default: raw.trips)
- `TAASIM_SPEED` (default: 10)
- `TAASIM_TRIPS_BASE_RATE_MAX` (default: 1.0)
- `TAASIM_TRIPS_TICK_S` (default: 0.5)
- `TAASIM_TIMEZONE` (default: Africa/Casablanca; falls back to UTC if unavailable)

Robustness notes
----------------
- Uses bounded work per tick via `--max-events-per-tick`.
- Handles SIGINT/SIGTERM with graceful shutdown and producer flush.
- Kafka sends are async; failures are logged via errback.

"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import random
import signal
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


def _iso8601(dt: datetime) -> str:
    """Return ISO-8601 in UTC with a trailing 'Z'."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


def _try_load_timezone(name: str) -> timezone:
    """Best-effort timezone loading.

    Uses `zoneinfo` when available. Falls back to UTC if the system tz database is missing.
    """

    if not name:
        return timezone.utc

    try:
        from zoneinfo import ZoneInfo  # py3.9+

        return ZoneInfo(name)
    except Exception:
        logger.warning("Timezone '%s' unavailable; falling back to UTC.", name)
        return timezone.utc


def _poisson(lam: float) -> int:
    """Sample from a Poisson distribution with mean `lam`.

    For small lambda we use Knuth's algorithm (exact).
    For larger lambda we use a normal approximation to avoid O(lam) runtime.

    Returns a non-negative integer.
    """

    if lam <= 0:
        return 0

    # Knuth exact sampler is fine up to this scale.
    if lam < 30.0:
        l = math.exp(-lam)
        k = 0
        p = 1.0
        while p > l:
            k += 1
            p *= random.random()
        return max(0, k - 1)

    # Normal approximation: N(lam, lam)
    value = random.gauss(lam, math.sqrt(lam))
    return max(0, int(value + 0.5))


DEFAULT_HOURLY_MULTIPLIERS: List[float] = [
    # 00  01   02   03   04   05   06   07   08   09   10   11
    0.22,
    0.18,
    0.15,
    0.14,
    0.16,
    0.22,
    0.35,
    0.85,
    1.00,
    0.75,
    0.55,
    0.50,
    # 12  13   14   15   16   17   18   19   20   21   22   23
    0.48,
    0.45,
    0.42,
    0.44,
    0.55,
    0.90,
    0.95,
    0.70,
    0.48,
    0.40,
    0.32,
    0.26,
]


def _validate_hourly_multipliers(multipliers: Sequence[float]) -> List[float]:
    if len(multipliers) != 24:
        raise ValueError("hourly multipliers must have exactly 24 values")

    validated: List[float] = []
    for v in multipliers:
        fv = float(v)
        if fv < 0:
            raise ValueError("hourly multipliers must be >= 0")
        validated.append(fv)

    if max(validated) <= 0:
        raise ValueError("hourly multipliers must contain at least one value > 0")

    return validated


def _day_multiplier(dt_local: datetime) -> float:
    """Return a multiplicative adjustment for weekday patterns.

    - Sunday reduced to ~0.6× overall
    - Friday 12–14 reduced to ~0.7×

    Note: weekday() => Monday=0, Friday=4, Sunday=6.
    """

    if dt_local.weekday() == 6:  # Sunday
        return 0.6

    if dt_local.weekday() == 4 and dt_local.hour in (12, 13, 14):  # Friday 12:00–14:59
        return 0.7

    return 1.0


def _weighted_choice(items: Sequence[str], weights: Sequence[float]) -> str:
    if len(items) != len(weights):
        raise ValueError("items and weights must have the same length")

    total = float(sum(weights))
    if total <= 0:
        raise ValueError("weights must sum to > 0")

    r = random.random() * total
    upto = 0.0
    for item, w in zip(items, weights):
        upto += float(w)
        if r <= upto:
            return item

    return items[-1]


@dataclass(frozen=True) # frozen makes the object read-only after creation
class ProducerSettings:
    broker: str
    topic: str
    timezone_name: str
    speed: float
    base_rate_max_per_sim_s: float
    tick_s: float
    zones: int
    rider_pool_size: int
    max_events_per_tick: int
    hourly_multipliers: List[float]


class TripRequestProducer:
    """Generates and publishes trip reservation events to Kafka."""

    def __init__(self, settings: ProducerSettings):
        if settings.speed <= 0:
            raise ValueError("speed must be > 0")
        if settings.base_rate_max_per_sim_s < 0:
            raise ValueError("base_rate_max_per_sim_s must be >= 0")
        if settings.tick_s <= 0:
            raise ValueError("tick_s must be > 0")
        if settings.zones < 2:
            raise ValueError("zones must be >= 2")
        if settings.rider_pool_size < 1:
            raise ValueError("rider_pool_size must be >= 1")
        if settings.max_events_per_tick < 1:
            raise ValueError("max_events_per_tick must be >= 1")

        self.settings = settings
        self._tz = _try_load_timezone(settings.timezone_name)

        self._producer = self._create_producer()
        self._stopping = False

    def _create_producer(self) -> KafkaProducer:
        try:
            return KafkaProducer(
                bootstrap_servers=[self.settings.broker],
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
                acks=1,
                retries=10,
                linger_ms=20,
                compression_type="gzip",
                client_id="taasim-trip-request-producer",
            )
        except Exception as exc:
            logger.error("Failed to create Kafka producer: %s", exc)
            raise

    def stop(self) -> None:
        self._stopping = True

    def _build_event(self, *, event_time_local: datetime) -> Dict[str, object]:
        trip_id = str(uuid.uuid4())
        rider_id = f"rider-{random.randint(1, self.settings.rider_pool_size)}"

        origin_zone = random.randint(1, self.settings.zones)
        dest_zone = random.randint(1, self.settings.zones)
        while dest_zone == origin_zone:
            dest_zone = random.randint(1, self.settings.zones)

        call_type = _weighted_choice(["A", "B", "C"], [0.35, 0.40, 0.25])

        return {
            "trip_id": trip_id,
            "rider_id": rider_id,
            "origin_zone": origin_zone,
            "destination_zone": dest_zone,
            "requested_at": _iso8601(event_time_local),
            "call_type": call_type,
        }

    def _send(self, *, key: str, value: Dict[str, object]) -> None:
        future = self._producer.send(self.settings.topic, key=key, value=value)

        def _on_error(exc: BaseException) -> None:
            logger.error("Kafka send failed: %s", exc)

        # kafka-python exposes add_errback for async error reporting.
        try:
            future.add_errback(_on_error)
        except Exception:
            # If callbacks aren't supported in this version, ignore.
            pass

    def run(self, *, start_time_local: Optional[datetime] = None) -> None:
        """Run until interrupted."""

        if start_time_local is None:
            start_time_local = datetime.now(self._tz)

        start_wall = time.monotonic()
        last_wall = start_wall
        last_sim = start_time_local

        produced_total = 0
        produced_last_log = 0
        last_log_wall = start_wall

        logger.info(
            "Starting TripRequestProducer: broker=%s topic=%s speed=%sx tz=%s",
            self.settings.broker,
            self.settings.topic,
            self.settings.speed,
            self.settings.timezone_name,
        )
        logger.info(
            "Rate model: base_max=%.3f ev/s(sim) tick=%.2fs zones=%d riders=%d",
            self.settings.base_rate_max_per_sim_s,
            self.settings.tick_s,
            self.settings.zones,
            self.settings.rider_pool_size,
        )

        try:
            while not self._stopping:
                time.sleep(self.settings.tick_s)

                now_wall = time.monotonic()
                wall_delta = now_wall - last_wall
                if wall_delta <= 0:
                    continue

                sim_delta_s = wall_delta * self.settings.speed
                sim_now = last_sim + timedelta(seconds=sim_delta_s)

                # Use the interval midpoint to pick the rate (good enough for short ticks).
                sim_mid = last_sim + timedelta(seconds=sim_delta_s / 2.0)
                mult_hour = self.settings.hourly_multipliers[sim_mid.hour]
                mult_day = _day_multiplier(sim_mid)
                rate_sim_per_s = self.settings.base_rate_max_per_sim_s * mult_hour * mult_day

                expected = rate_sim_per_s * sim_delta_s
                n_events = _poisson(expected)
                if n_events > self.settings.max_events_per_tick:
                    logger.warning(
                        "Capping events per tick: sampled=%d cap=%d (expected=%.2f)",
                        n_events,
                        self.settings.max_events_per_tick,
                        expected,
                    )
                    n_events = self.settings.max_events_per_tick

                for _ in range(n_events):
                    # Randomize event times inside the simulated interval.
                    offset_s = random.random() * sim_delta_s
                    event_time = last_sim + timedelta(seconds=offset_s)

                    payload = self._build_event(event_time_local=event_time)
                    self._send(key=str(payload["trip_id"]), value=payload)

                produced_total += n_events

                # Progress log once per ~10 seconds.
                if now_wall - last_log_wall >= 10.0:
                    produced_since = produced_total - produced_last_log
                    produced_last_log = produced_total
                    last_log_wall = now_wall
                    logger.info(
                        "Produced %d events (total=%d). sim_time=%s rate=%.2f ev/s(sim)",
                        produced_since,
                        produced_total,
                        sim_now.isoformat(timespec="seconds"),
                        rate_sim_per_s,
                    )

                last_wall = now_wall
                last_sim = sim_now

        except KeyboardInterrupt:
            logger.info("Interrupted by user; stopping...")
        finally:
            logger.info("Flushing Kafka producer...")
            try:
                self._producer.flush(timeout=10)
            except Exception:
                pass
            try:
                self._producer.close(timeout=10)
            except Exception:
                pass
            logger.info("TripRequestProducer stopped. Total events produced=%d", produced_total)


def _minio_storage_options() -> Dict[str, object]:
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint_url:
        endpoint_url = "http://localhost:9000"
        logger.warning("AWS_ENDPOINT_URL not set; defaulting to %s", endpoint_url)

    return {
        "key": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "client_kwargs": {"endpoint_url": endpoint_url},
        "config_kwargs": {"s3": {"addressing_style": "path"}},
    }


def compute_hourly_multipliers_from_porto_train_csv(path: str, *, tz_name: str) -> List[float]:
    """Compute a 24-value demand multiplier list from Porto `train.csv`.

    Reads the `TIMESTAMP` column and counts trips per hour-of-day.
    The result is normalized by the maximum count so max(hour)=1.0.

    Supports local paths and `s3a://...`/`s3://...` via fsspec.
    """

    tz = _try_load_timezone(tz_name)

    open_path = path
    storage_options: Optional[Dict[str, object]] = None

    if open_path.startswith("s3a://"):
        open_path = "s3://" + open_path[len("s3a://") :]
    if open_path.startswith("s3://"):
        storage_options = _minio_storage_options()

    def _open_file() -> Iterable[str]:
        if open_path.startswith("s3://"):
            import fsspec

            with fsspec.open(open_path, mode="rt", encoding="utf-8", newline="", **storage_options) as f:
                yield from f
        else:
            with open(open_path, mode="rt", encoding="utf-8", newline="") as f:
                yield from f

    counts = [0] * 24
    reader = csv.DictReader(_open_file())
    for row in reader:
        try:
            ts = int(row["TIMESTAMP"])
        except Exception:
            continue

        dt_local = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz)
        counts[dt_local.hour] += 1

    max_count = max(counts)
    if max_count <= 0:
        raise ValueError(f"No valid TIMESTAMP values read from: {path}")

    return [round(c / max_count, 4) for c in counts]


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TaaSim Trip Request Producer (raw.trips)")

    p.add_argument("--broker", default=os.environ.get("TAASIM_KAFKA_BROKER", "localhost:9092"))
    p.add_argument("--topic", default=os.environ.get("TAASIM_TRIPS_TOPIC", "raw.trips"))

    p.add_argument("--speed", type=float, default=float(os.environ.get("TAASIM_SPEED", "10")))
    p.add_argument(
        "--base-rate-max",
        type=float,
        default=float(os.environ.get("TAASIM_TRIPS_BASE_RATE_MAX", "1.0")),
        help="Max events per simulated second when hourly multiplier is 1.0.",
    )
    p.add_argument(
        "--tick-s",
        type=float,
        default=float(os.environ.get("TAASIM_TRIPS_TICK_S", "0.5")),
        help="Wall-clock tick size (seconds). Smaller = smoother rate changes.",
    )

    p.add_argument(
        "--timezone",
        default=os.environ.get("TAASIM_TIMEZONE", "Africa/Casablanca"),
        help="Timezone used for hour-of-day and weekday patterns.",
    )

    p.add_argument(
        "--zones",
        type=int,
        default=int(os.environ.get("TAASIM_ZONES", "16")),
        help="Number of Casablanca zones (default: 16).",
    )

    p.add_argument(
        "--rider-pool-size",
        type=int,
        default=int(os.environ.get("TAASIM_RIDER_POOL_SIZE", "2000")),
        help="Number of distinct rider IDs to sample.",
    )

    p.add_argument(
        "--max-events-per-tick",
        type=int,
        default=int(os.environ.get("TAASIM_MAX_EVENTS_PER_TICK", "500")),
        help="Safety cap to avoid runaway event storms.",
    )

    p.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("TAASIM_SEED", "0")),
        help="PRNG seed (0 = non-deterministic).",
    )

    p.add_argument(
        "--start-iso",
        default=os.environ.get("TAASIM_START_ISO"),
        help="Start simulated time (ISO-8601). If omitted, uses now().",
    )

    p.add_argument(
        "--compute-hourly-multipliers",
        metavar="TRAIN_CSV_PATH",
        help="Compute Porto hourly multipliers from train.csv and print the 24-value list, then exit.",
    )

    return p


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    args = _build_arg_parser().parse_args()

    if args.seed:
        random.seed(args.seed)

    if args.compute_hourly_multipliers:
        multipliers = compute_hourly_multipliers_from_porto_train_csv(args.compute_hourly_multipliers, tz_name=args.timezone)
        print(multipliers)
        return

    hourly_multipliers = _validate_hourly_multipliers(DEFAULT_HOURLY_MULTIPLIERS)

    start_time_local: Optional[datetime] = None
    if args.start_iso:
        # Robust parsing with a minimal dependency footprint.
        iso = str(args.start_iso).strip()
        if iso.endswith("Z"):
            iso = iso[:-1] + "+00:00"
        start_time_local = datetime.fromisoformat(iso)
        if start_time_local.tzinfo is None:
            start_time_local = start_time_local.replace(tzinfo=_try_load_timezone(args.timezone))
        else:
            start_time_local = start_time_local.astimezone(_try_load_timezone(args.timezone))

    settings = ProducerSettings(
        broker=args.broker,
        topic=args.topic,
        timezone_name=args.timezone,
        speed=args.speed,
        base_rate_max_per_sim_s=args.base_rate_max,
        tick_s=args.tick_s,
        zones=args.zones,
        rider_pool_size=args.rider_pool_size,
        max_events_per_tick=args.max_events_per_tick,
        hourly_multipliers=hourly_multipliers,
    )

    producer = TripRequestProducer(settings)

    def _handle_signal(signum: int, _frame: object) -> None:
        logger.info("Received signal %s; stopping...", signum)
        producer.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    producer.run(start_time_local=start_time_local)


if __name__ == "__main__":
    main()
