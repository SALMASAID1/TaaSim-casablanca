import argparse
import csv
import heapq
import json
import logging
import math
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _unix_to_iso8601(ts: int) -> str:
    # ISO-8601 (UTC) — friendly for Flink timestamp assigners.
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _porto_to_casablanca(lon: float, lat: float) -> Tuple[float, float]:
    """Linear bounding-box mapping (Task 04) from Porto -> Casablanca."""
    p_lon_min, p_lon_max = -8.7, -8.5
    p_lat_min, p_lat_max = 41.1, 41.2

    c_lon_min, c_lon_max = -7.8, -7.4
    c_lat_min, c_lat_max = 33.4, 33.7

    cas_lon = c_lon_min + (lon - p_lon_min) / (p_lon_max - p_lon_min) * (c_lon_max - c_lon_min)
    cas_lat = c_lat_min + (lat - p_lat_min) / (p_lat_max - p_lat_min) * (c_lat_max - c_lat_min)
    return cas_lon, cas_lat


def _haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Great-circle distance in km."""
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


class _DelayedKafkaSender:
    """Sends Kafka messages after a wall-clock delay without blocking the main loop."""

    def __init__(self, producer: KafkaProducer, topic: str):
        self._producer = producer
        self._topic = topic
        self._cv = threading.Condition()
        self._queue: List[Tuple[float, int, str, dict]] = []
        self._seq = 0
        self._stopping = False
        self._thread = threading.Thread(target=self._run, name="delayed-kafka-sender", daemon=True)
        self._thread.start()

    def schedule(self, delay_seconds: float, *, key: str, value: dict) -> None:
        send_at = time.monotonic() + max(0.0, delay_seconds)
        with self._cv:
            self._seq += 1
            heapq.heappush(self._queue, (send_at, self._seq, key, value))
            self._cv.notify()

    def _run(self) -> None:
        while True:
            with self._cv:
                while not self._stopping and not self._queue:
                    self._cv.wait()

                if self._stopping and not self._queue:
                    return

                send_at, _, key, value = self._queue[0]
                now = time.monotonic()

                if not self._stopping and send_at > now:
                    self._cv.wait(timeout=send_at - now)
                    continue

                heapq.heappop(self._queue)

            try:
                self._producer.send(self._topic, key=key, value=value)
            except Exception as exc:
                logger.error("Delayed send failed: %s", exc)

    def stop(self) -> None:
        with self._cv:
            self._stopping = True
            self._cv.notify_all()
        self._thread.join(timeout=30)


@dataclass(frozen=True)
class _Trip:
    trip_id: str
    taxi_id: str
    base_ts: int
    polyline: List[Tuple[float, float]]  # list of (porto_lon, porto_lat)


class VehicleGPSProducer:
    """Replays taxi GPS events into Kafka (topic raw.gps).

    Supports two input formats:
    - Porto raw CSV (train.csv) with POLYLINE of [lon,lat] points.
    - Pre-exploded parquet with columns: TIMESTAMP, TAXI_ID, cas_lat, cas_lon (optional TRIP_ID).
    """

    def __init__(
        self,
        *,
        broker: str,
        topic: str,
        data_path: str,
        speed: float = 10.0,
        noise_sigma_deg: float = 0.0002,
        blackout_prob: float = 0.05,
        blackout_delay_min_s: float = 60.0,
        blackout_delay_max_s: float = 180.0,
        max_trips: int = 200,
        loop: bool = True,
    ):
        if speed <= 0:
            raise ValueError("speed must be > 0")
        if noise_sigma_deg < 0:
            raise ValueError("noise_sigma_deg must be >= 0")
        if not (0.0 <= blackout_prob <= 1.0):
            raise ValueError("blackout_prob must be between 0 and 1")
        if blackout_delay_min_s < 0:
            raise ValueError("blackout_delay_min_s must be >= 0")
        if blackout_delay_max_s < 0:
            raise ValueError("blackout_delay_max_s must be >= 0")
        if blackout_delay_max_s < blackout_delay_min_s:
            raise ValueError("blackout_delay_max_s must be >= blackout_delay_min_s")

        self.broker = broker
        self.topic = topic
        self.data_path = data_path
        self.speed = speed
        self.noise_sigma_deg = noise_sigma_deg
        self.blackout_prob = blackout_prob
        self.blackout_delay_min_s = blackout_delay_min_s
        self.blackout_delay_max_s = blackout_delay_max_s
        self.max_trips = max_trips
        self.loop = loop

        self.producer = self._create_producer()
        self._delayed_sender = _DelayedKafkaSender(self.producer, self.topic)

    def _create_producer(self) -> KafkaProducer:
        try:
            return KafkaProducer(
                bootstrap_servers=[self.broker],
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
                acks=1,
                retries=5,
                linger_ms=20,
                compression_type="gzip",
            )
        except Exception as exc:
            logger.error("Failed to connect to Kafka: %s", exc)
            raise

    def _minio_storage_options(self) -> Dict[str, object]:
        endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
        if not endpoint_url:
            if self.broker.startswith(("localhost", "127.0.0.1")):
                endpoint_url = "http://localhost:9000"
            else:
                endpoint_url = "http://minio:9000"
            logger.warning("AWS_ENDPOINT_URL not set; defaulting to %s", endpoint_url)

        return {
            "key": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            "client_kwargs": {"endpoint_url": endpoint_url},
            "config_kwargs": {"s3": {"addressing_style": "path"}},
        }

    def _apply_gaussian_noise(self, lat: float, lon: float) -> Tuple[float, float]:
        noisy_lat = random.gauss(lat, self.noise_sigma_deg)
        noisy_lon = random.gauss(lon, self.noise_sigma_deg)
        return round(noisy_lat, 6), round(noisy_lon, 6)

    def _maybe_send_with_blackout(self, *, taxi_id: str, payload: dict) -> None:
        if random.random() < self.blackout_prob:
            delay_event_s = random.uniform(self.blackout_delay_min_s, self.blackout_delay_max_s)
            delay_wall_s = delay_event_s / self.speed
            self._delayed_sender.schedule(delay_wall_s, key=taxi_id, value=payload)
        else:
            self.producer.send(self.topic, key=taxi_id, value=payload)

    def _load_porto_trips(self, path: str) -> List[_Trip]:
        storage_options: Optional[Dict[str, object]] = None
        open_path = path

        if open_path.startswith("s3a://"):
            open_path = "s3://" + open_path[len("s3a://") :]
        if open_path.startswith("s3://"):
            storage_options = self._minio_storage_options()

        def _open_file() -> Iterable[str]:
            if open_path.startswith("s3://"):
                import fsspec

                with fsspec.open(open_path, mode="rt", encoding="utf-8", newline="", **storage_options) as f:
                    yield from f
            else:
                with open(open_path, mode="rt", encoding="utf-8", newline="") as f:
                    yield from f

        trips: List[_Trip] = []
        reader = csv.DictReader(_open_file())
        for row in reader:
            if len(trips) >= self.max_trips:
                break

            if str(row.get("MISSING_DATA", "")).strip().lower() == "true":
                continue

            polyline_raw = row.get("POLYLINE")
            if not polyline_raw:
                continue

            try:
                coords = json.loads(polyline_raw)
            except Exception:
                continue

            if not coords:
                continue

            try:
                base_ts = int(row["TIMESTAMP"])
            except Exception:
                continue

            polyline: List[Tuple[float, float]] = []
            for p in coords:
                if not isinstance(p, list) or len(p) != 2:
                    continue
                try:
                    polyline.append((float(p[0]), float(p[1])))
                except Exception:
                    continue

            if not polyline:
                continue

            trips.append(
                _Trip(
                    trip_id=str(row.get("TRIP_ID", "")),
                    taxi_id=str(row.get("TAXI_ID", "")),
                    base_ts=base_ts,
                    polyline=polyline,
                )
            )

        if not trips:
            raise ValueError(f"No valid trips found in {path}")

        return trips

    def _stream_from_porto_csv(self) -> None:
        logger.info("Loading Porto CSV trips from %s...", self.data_path)
        trips = self._load_porto_trips(self.data_path)
        logger.info("Loaded %d trips; building event heap...", len(trips))

        min_event_ts = min(t.base_ts for t in trips)

        while True:
            sim_start = int(time.time())
            heap: List[Tuple[int, int, int]] = []  # (event_ts, trip_idx, point_idx)
            for trip_idx, trip in enumerate(trips):
                heapq.heappush(heap, (trip.base_ts, trip_idx, 0))

            last_event_ts: Optional[int] = None
            produced = 0

            while heap:
                event_ts, trip_idx, point_idx = heapq.heappop(heap)

                if last_event_ts is not None:
                    wait_s = (event_ts - last_event_ts) / self.speed
                    if wait_s > 0:
                        time.sleep(wait_s)

                trip = trips[trip_idx]
                porto_lon, porto_lat = trip.polyline[point_idx]
                cas_lon, cas_lat = _porto_to_casablanca(porto_lon, porto_lat)

                # Speed within trip (15s per ping).
                speed_kmh = 0.0
                if point_idx > 0:
                    prev_lon, prev_lat = trip.polyline[point_idx - 1]
                    prev_cas_lon, prev_cas_lat = _porto_to_casablanca(prev_lon, prev_lat)
                    dist_km = _haversine_km(prev_cas_lon, prev_cas_lat, cas_lon, cas_lat)
                    speed_kmh = float((dist_km / 15.0) * 3600.0) if dist_km > 0 else 0.0

                noisy_lat, noisy_lon = self._apply_gaussian_noise(cas_lat, cas_lon)

                rebased_ts = sim_start + (event_ts - min_event_ts)
                payload = {
                    "taxi_id": trip.taxi_id,
                    "timestamp": _unix_to_iso8601(rebased_ts),
                    "lat": noisy_lat,
                    "lon": noisy_lon,
                    "speed": round(speed_kmh, 2),
                    "status": "available",
                    "trip_id": trip.trip_id,
                }

                self._maybe_send_with_blackout(taxi_id=trip.taxi_id, payload=payload)
                produced += 1
                if produced % 10_000 == 0:
                    logger.info("Produced %d GPS events...", produced)

                last_event_ts = event_ts

                next_idx = point_idx + 1
                if next_idx < len(trip.polyline):
                    heapq.heappush(heap, (trip.base_ts + (next_idx * 15), trip_idx, next_idx))

            if not self.loop:
                logger.info("Completed one pass of %d events; exiting.", produced)
                return

            logger.info("Completed one pass (%d events). Looping...", produced)

    def _stream_from_parquet(self) -> None:
        logger.info("Loading parquet from %s...", self.data_path)
        path = self.data_path
        storage_options: Optional[Dict[str, object]] = None

        # Pandas/pyarrow uses fsspec/s3fs, which expects s3:// (not s3a://)
        if path.startswith("s3a://"):
            path = "s3://" + path[len("s3a://") :]
        if path.startswith("s3://"):
            storage_options = self._minio_storage_options()

        df = pd.read_parquet(
            path,
            columns=["TIMESTAMP", "TRIP_ID", "TAXI_ID", "cas_lat", "cas_lon"],
            storage_options=storage_options,
        ).sort_values(by="TIMESTAMP")

        if df.empty:
            raise ValueError(f"Parquet dataset is empty: {self.data_path}")

        base_ts = int(df["TIMESTAMP"].min())

        logger.info("Broadcasting at %sx speed (parquet replay)...", self.speed)

        last_stream_ts: Optional[int] = None
        sim_start = int(time.time())
        last_by_taxi: Dict[str, Tuple[int, float, float]] = {}
        produced = 0

        try:
            for row in df.itertuples(index=False):
                curr_ts_raw = int(getattr(row, "TIMESTAMP"))
                taxi_id = str(getattr(row, "TAXI_ID"))
                trip_id = str(getattr(row, "TRIP_ID"))
                cas_lat = float(getattr(row, "cas_lat"))
                cas_lon = float(getattr(row, "cas_lon"))

                # Sleep by event-time gap (accelerated).
                if last_stream_ts is not None:
                    wait_s = (curr_ts_raw - last_stream_ts) / self.speed
                    if wait_s > 0:
                        time.sleep(wait_s)

                # Speed based on last point seen for this taxi.
                speed_kmh = 0.0
                if taxi_id in last_by_taxi:
                    prev_ts, prev_lat, prev_lon = last_by_taxi[taxi_id]
                    dt = curr_ts_raw - prev_ts
                    if dt > 0:
                        dist_km = _haversine_km(prev_lon, prev_lat, cas_lon, cas_lat)
                        speed_kmh = float((dist_km / dt) * 3600.0)

                last_by_taxi[taxi_id] = (curr_ts_raw, cas_lat, cas_lon)
                noisy_lat, noisy_lon = self._apply_gaussian_noise(cas_lat, cas_lon)

                rebased_ts = sim_start + (curr_ts_raw - base_ts)
                payload = {
                    "taxi_id": taxi_id,
                    "timestamp": _unix_to_iso8601(rebased_ts),
                    "lat": noisy_lat,
                    "lon": noisy_lon,
                    "speed": round(speed_kmh, 2),
                    "status": "available",
                    "trip_id": trip_id,
                }

                self._maybe_send_with_blackout(taxi_id=taxi_id, payload=payload)
                produced += 1
                if produced % 10_000 == 0:
                    logger.info("Produced %d GPS events...", produced)

                last_stream_ts = curr_ts_raw

        except KeyboardInterrupt:
            logger.info("Simulation stopped by user.")
        finally:
            logger.info("Produced %d events total.", produced)

    def start(self) -> None:
        try:
            if self.data_path.lower().endswith(".csv"):
                self._stream_from_porto_csv()
            else:
                self._stream_from_parquet()
        finally:
            self.stop()

    def stop(self) -> None:
        logger.info("Stopping producer: draining delayed sends, flushing, closing...")
        try:
            self._delayed_sender.stop()
        except Exception:
            pass
        self.producer.flush()
        self.producer.close()


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="TaaSim Vehicle GPS Producer")
    p.add_argument("--broker", default=os.environ.get("TAASIM_KAFKA_BROKER", "localhost:9092"))
    p.add_argument("--topic", default=os.environ.get("TAASIM_KAFKA_TOPIC", "raw.gps"))
    p.add_argument(
        "--data-path",
        default=os.environ.get("TAASIM_DATA_PATH", "raw/porto-trips/train.csv"),
        help="Either Porto train.csv (local or s3a://...) or curated parquet directory (s3a://...).",
    )
    p.add_argument("--speed", type=float, default=float(os.environ.get("TAASIM_SPEED", "10.0")))
    p.add_argument(
        "--noise-sigma-deg",
        type=float,
        default=float(os.environ.get("TAASIM_NOISE_SIGMA_DEG", "0.0002")),
        help="Gaussian noise sigma in degrees (~0.0002 ≈ 20m).",
    )
    p.add_argument(
        "--blackout-prob",
        type=float,
        default=float(os.environ.get("TAASIM_BLACKOUT_PROB", "0.05")),
        help="Probability that an event is delayed (late / out-of-order).",
    )
    p.add_argument(
        "--blackout-delay-min-s",
        type=float,
        default=float(os.environ.get("TAASIM_BLACKOUT_DELAY_MIN_S", "60.0")),
        help="Minimum event-time delay (seconds) for delayed events.",
    )
    p.add_argument(
        "--blackout-delay-max-s",
        type=float,
        default=float(os.environ.get("TAASIM_BLACKOUT_DELAY_MAX_S", "180.0")),
        help="Maximum event-time delay (seconds) for delayed events.",
    )
    p.add_argument("--max-trips", type=int, default=int(os.environ.get("TAASIM_MAX_TRIPS", "200")))
    p.add_argument(
        "--once",
        action="store_true",
        help="Run one pass through the input data then exit (CSV mode only).",
    )
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()

    producer_service = VehicleGPSProducer(
        broker=args.broker,
        topic=args.topic,
        data_path=args.data_path,
        speed=args.speed,
        noise_sigma_deg=args.noise_sigma_deg,
        blackout_prob=args.blackout_prob,
        blackout_delay_min_s=args.blackout_delay_min_s,
        blackout_delay_max_s=args.blackout_delay_max_s,
        max_trips=args.max_trips,
        loop=not args.once,
    )
    producer_service.start()

