import argparse
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
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _BBox:
    lon_min: float
    lon_max: float
    lat_min: float
    lat_max: float


# Target bbox: union of metadata/zone_mapping.csv (maximizes zone join success)
_CASABLANCA_BBOX = _BBox(lon_min=-7.730, lon_max=-7.480, lat_min=33.510, lat_max=33.645)


def _clamp(value: float, lo: float, hi: float) -> float:
    return lo if value < lo else hi if value > hi else value


def _clamp_lon_lat_to_bbox(lon: float, lat: float, bbox: _BBox) -> Tuple[float, float]:
    return (
        _clamp(lon, bbox.lon_min, bbox.lon_max),
        _clamp(lat, bbox.lat_min, bbox.lat_max),
    )


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


def _polyline_length_km(points: List[Tuple[float, float]]) -> float:
    if len(points) < 2:
        return 0.0
    total = 0.0
    for (lon1, lat1), (lon2, lat2) in zip(points[:-1], points[1:]):
        total += _haversine_km(lon1, lat1, lon2, lat2)
    return total


def _resample_polyline(points: List[Tuple[float, float]], n_points: int) -> List[Tuple[float, float]]:
    """Resample a polyline to exactly n_points, preserving endpoints.

    Used to convert dense road-geometry polylines into realistic GPS ping series.
    """

    if not points:
        return []
    if n_points <= 1:
        return [points[0]]
    if len(points) == 1:
        return [points[0]] * n_points
    if n_points == 2:
        return [points[0], points[-1]]

    # Build cumulative distance array (km).
    cum: List[float] = [0.0]
    for (lon1, lat1), (lon2, lat2) in zip(points[:-1], points[1:]):
        cum.append(cum[-1] + _haversine_km(lon1, lat1, lon2, lat2))

    total = cum[-1]
    if total <= 0.0:
        return [points[0]] * (n_points - 1) + [points[-1]]

    targets = [i * total / (n_points - 1) for i in range(n_points)]
    out: List[Tuple[float, float]] = []
    seg_idx = 0

    for d in targets:
        while seg_idx < len(cum) - 2 and cum[seg_idx + 1] < d:
            seg_idx += 1

        d0 = cum[seg_idx]
        d1 = cum[seg_idx + 1]
        if d1 <= d0:
            out.append(points[seg_idx])
            continue

        f = (d - d0) / (d1 - d0)
        lon0, lat0 = points[seg_idx]
        lon1, lat1 = points[seg_idx + 1]
        out.append((lon0 + f * (lon1 - lon0), lat0 + f * (lat1 - lat0)))

    out[0] = points[0]
    out[-1] = points[-1]
    return out


def _as_positive_float(value: object) -> Optional[float]:
    try:
        if value is None:
            return None
        f = float(value)
        if not math.isfinite(f) or f <= 0.0:
            return None
        return f
    except Exception:
        return None


def _repo_root() -> Path:
    # File lives at: <repo>/src/producers/vehicle_gps_producer.py
    return Path(__file__).resolve().parents[2]


def _resolve_data_path(path: str) -> str:
    """Resolve a local relative path.

    S3 paths are returned unchanged.
    """

    if not path:
        return path

    if path.startswith(("s3a://", "s3://")):
        return path

    p = Path(path)
    if p.is_absolute():
        return str(p)

    cwd_candidate = Path.cwd() / p
    if cwd_candidate.exists():
        return str(cwd_candidate.resolve())

    repo_candidate = _repo_root() / p
    if repo_candidate.exists():
        resolved = str(repo_candidate.resolve())
        logger.info("Resolved data path '%s' -> '%s' (repo-relative).", path, resolved)
        return resolved

    return path


def _unix_to_iso8601(ts: int) -> str:
    # ISO-8601 (UTC) — friendly for Flink timestamp assigners.
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


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
    polyline: List[Tuple[float, float]]  # pre-mapped (lon, lat) in Casablanca BBox


class VehicleGPSProducer:
    """Replays pre-mapped Casablanca taxi GPS events from MinIO Parquet files into Kafka."""

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
        clamp_bbox: bool = True,
        parquet_avg_speed_kmh: float = 25.0,
        parquet_max_pings: int = 400,
        **kwargs,  # Gracefully ignore deprecated/obsolete kwargs (like mapping_mode, casa_place, casa_graphml)
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
        self.data_path = _resolve_data_path(data_path)
        self.speed = speed
        self.noise_sigma_deg = noise_sigma_deg
        self.blackout_prob = blackout_prob
        self.blackout_delay_min_s = blackout_delay_min_s
        self.blackout_delay_max_s = blackout_delay_max_s
        self.max_trips = max_trips
        self.loop = loop
        self.clamp_bbox = clamp_bbox
        self.parquet_avg_speed_kmh = parquet_avg_speed_kmh
        self.parquet_max_pings = parquet_max_pings

        if kwargs:
            logger.info("Gracefully ignored deprecated parameters: %s", list(kwargs.keys()))

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
        if self.noise_sigma_deg > 0:
            noisy_lat = random.gauss(lat, self.noise_sigma_deg)
            noisy_lon = random.gauss(lon, self.noise_sigma_deg)
        else:
            noisy_lat, noisy_lon = lat, lon

        if self.clamp_bbox:
            noisy_lon, noisy_lat = _clamp_lon_lat_to_bbox(noisy_lon, noisy_lat, _CASABLANCA_BBOX)

        return round(noisy_lat, 6), round(noisy_lon, 6)

    def _maybe_send_with_blackout(self, *, taxi_id: str, payload: dict) -> None:
        if random.random() < self.blackout_prob:
            delay_event_s = random.uniform(self.blackout_delay_min_s, self.blackout_delay_max_s)
            delay_wall_s = delay_event_s / self.speed
            self._delayed_sender.schedule(delay_wall_s, key=taxi_id, value=payload)
        else:
            self.producer.send(self.topic, key=taxi_id, value=payload)

    def _stream_from_parquet(self) -> None:
        logger.info("Loading Parquet from %s...", self.data_path)
        path = self.data_path
        storage_options: Optional[Dict[str, object]] = None

        if path.startswith("s3a://"):
            path = "s3://" + path[len("s3a://") :]
        if path.startswith("s3://"):
            storage_options = self._minio_storage_options()

        base_cols = ["trip_id", "taxi_id", "timestamp", "polyline"]
        candidate_cols = [
            base_cols + ["duration_sec", "distance_km"],
            base_cols + ["duration_sec"],
            base_cols + ["distance_km"],
            base_cols,
        ]

        last_err: Optional[Exception] = None
        df = None
        for cols in candidate_cols:
            try:
                df = pd.read_parquet(
                    path,
                    columns=cols,
                    storage_options=storage_options,
                )
                break
            except Exception as exc:
                last_err = exc
                continue

        if df is None:
            assert last_err is not None
            raise last_err

        if "timestamp" in df.columns:
            df = df.sort_values(by="timestamp")

        if df.empty:
            raise ValueError(f"Parquet dataset is empty: {self.data_path}")

        trips: List[_Trip] = []
        for row in df.itertuples(index=False):
            if len(trips) >= self.max_trips:
                break

            try:
                poly_val = getattr(row, "polyline")
                if poly_val is None:
                    continue

                if isinstance(poly_val, (bytes, bytearray)):
                    poly_val = poly_val.decode("utf-8")

                if isinstance(poly_val, str):
                    coords = json.loads(poly_val)
                else:
                    coords = poly_val

                if not isinstance(coords, list) and hasattr(coords, "tolist"):
                    coords = coords.tolist()
                elif not isinstance(coords, list) and isinstance(coords, tuple):
                    coords = list(coords)

                if not isinstance(coords, list) or not coords:
                    continue

                raw_polyline: List[Tuple[float, float]] = []
                for p in coords:
                    if isinstance(p, (str, bytes, bytearray)):
                        continue
                    if hasattr(p, "__len__") and len(p) == 2:
                        try:
                            raw_polyline.append((float(p[0]), float(p[1])))
                        except Exception:
                            continue

                if len(raw_polyline) < 2:
                    continue

                duration_sec = _as_positive_float(getattr(row, "duration_sec", None))
                distance_km = _as_positive_float(getattr(row, "distance_km", None))

                n_pings: Optional[int] = None
                if duration_sec is not None:
                    n_pings = int(round(duration_sec / 15.0)) + 1
                elif distance_km is not None:
                    est_duration = (distance_km / max(self.parquet_avg_speed_kmh, 1.0)) * 3600.0
                    n_pings = int(round(est_duration / 15.0)) + 1
                else:
                    est_km = _polyline_length_km(raw_polyline)
                    est_duration = (est_km / max(self.parquet_avg_speed_kmh, 1.0)) * 3600.0
                    n_pings = int(round(est_duration / 15.0)) + 1

                n_pings = max(2, min(int(n_pings), int(self.parquet_max_pings)))
                polyline = _resample_polyline(raw_polyline, n_pings)
                if self.clamp_bbox:
                    polyline = [
                        _clamp_lon_lat_to_bbox(lon, lat, _CASABLANCA_BBOX)
                        for lon, lat in polyline
                    ]

                trips.append(
                    _Trip(
                        trip_id=str(getattr(row, "trip_id", "")),
                        taxi_id=str(getattr(row, "taxi_id", "")),
                        base_ts=int(getattr(row, "timestamp")),
                        polyline=polyline,
                    )
                )
            except Exception:
                continue

        if not trips:
            raise ValueError(f"No valid trips found in {path}")

        self._replay_trips(trips)

    def _replay_trips(self, trips: List[_Trip]) -> None:
        logger.info("Loaded %d trips; building event heap for %sx speed replay...", len(trips), self.speed)

        min_event_ts = min(t.base_ts for t in trips)

        try:
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
                    lon, lat = trip.polyline[point_idx]
                    cas_lon, cas_lat = lon, lat

                    # Speed within trip (15s per ping).
                    speed_kmh = 0.0
                    if point_idx > 0:
                        prev_lon, prev_lat = trip.polyline[point_idx - 1]
                        prev_cas_lon, prev_cas_lat = prev_lon, prev_lat
                        dist_km = _haversine_km(prev_cas_lon, prev_cas_lat, cas_lon, cas_lat)
                        speed_kmh = float((dist_km / 15.0) * 3600.0) if dist_km > 0 else 0.0

                    noisy_lat, noisy_lon = self._apply_gaussian_noise(cas_lat, cas_lon)

                    # Rebase event time to wall clock
                    rebased_ts = sim_start + (event_ts - min_event_ts) / self.speed
                    payload = {
                        "taxi_id": trip.taxi_id,
                        "timestamp": _unix_to_iso8601(int(rebased_ts)),
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

        except KeyboardInterrupt:
            logger.info("Simulation stopped by user.")
        finally:
            logger.info("Produced %d events total.", produced)

    def start(self) -> None:
        try:
            if self.data_path.lower().endswith(".csv"):
                raise ValueError(
                    "Porto raw CSV format is no longer supported in this optimized version. "
                    "Please use the pre-mapped MinIO Parquet trajectories."
                )
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
        default=os.environ.get("TAASIM_DATA_PATH", "s3://taasim/curated/mapped_casa_trips/"),
        help="S3 parquet directory (s3a://...) containing pre-mapped Casablanca trajectories.",
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
        "--parquet-avg-speed-kmh",
        type=float,
        default=float(os.environ.get("TAASIM_PARQUET_AVG_SPEED_KMH", "25.0")),
        help="Used to estimate ping count when parquet lacks duration_sec.",
    )
    p.add_argument(
        "--parquet-max-pings",
        type=int,
        default=int(os.environ.get("TAASIM_PARQUET_MAX_PINGS", "400")),
        help="Upper bound on number of pings emitted per trip for parquet inputs.",
    )
    p.add_argument(
        "--clamp-bbox",
        dest="clamp_bbox",
        action="store_true",
        help="Clamp emitted coordinates into the Casablanca bbox (default).",
    )
    p.add_argument(
        "--no-clamp-bbox",
        dest="clamp_bbox",
        action="store_false",
        help="Do not clamp emitted coordinates.",
    )
    p.set_defaults(clamp_bbox=True)
    p.add_argument(
        "--once",
        action="store_true",
        help="Run one pass through the input data then exit.",
    )

    # Obsolete kwargs fallbacks to avoid breaking legacy shell execution scripts
    p.add_argument("--mapping-mode", default="road", help="Deprecated/Ignored")
    p.add_argument("--casa-place", default="Casablanca, Morocco", help="Deprecated/Ignored")
    p.add_argument("--casa-graphml", default="/tmp/casablanca_drive.graphml", help="Deprecated/Ignored")

    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    logger.info("Initializing GPS Producer with args: %s", args)
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
        clamp_bbox=args.clamp_bbox,
        parquet_avg_speed_kmh=args.parquet_avg_speed_kmh,
        parquet_max_pings=args.parquet_max_pings,
        # Deprecated kwargs are consumed by **kwargs in init
        mapping_mode=args.mapping_mode,
        casa_place=args.casa_place,
        casa_graphml=args.casa_graphml,
    )
    producer_service.start()
