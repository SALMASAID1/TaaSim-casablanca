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


# Capstone brief + Job1 validation bbox constants (keep aligned with docs).
_PORTO_BBOX = _BBox(lon_min=-8.7, lon_max=-8.5, lat_min=41.1, lat_max=41.2)
_CASABLANCA_BBOX = _BBox(lon_min=-7.8, lon_max=-7.4, lat_min=33.4, lat_max=33.7)


def _clamp(value: float, lo: float, hi: float) -> float:
    return lo if value < lo else hi if value > hi else value


def _clamp01(value: float) -> float:
    return _clamp(value, 0.0, 1.0)


def _clamp_lon_lat_to_bbox(lon: float, lat: float, bbox: _BBox) -> Tuple[float, float]:
    return (
        _clamp(lon, bbox.lon_min, bbox.lon_max),
        _clamp(lat, bbox.lat_min, bbox.lat_max),
    )


def _affine_bbox_map(lon: float, lat: float, src: _BBox, dst: _BBox) -> Tuple[float, float]:
    """Relative-position affine map with clamping to [0,1] (Notebook 03 ADR-01).

    A point at 30% across the source bbox maps to 30% across the destination bbox.
    """

    lon_den = src.lon_max - src.lon_min
    lat_den = src.lat_max - src.lat_min
    if lon_den == 0.0 or lat_den == 0.0:
        # Defensive fallback; should never happen for our fixed bboxes.
        return dst.lon_min, dst.lat_min

    rel_lon = (lon - src.lon_min) / lon_den
    rel_lat = (lat - src.lat_min) / lat_den
    rel_lon = _clamp01(rel_lon)
    rel_lat = _clamp01(rel_lat)

    mapped_lon = dst.lon_min + rel_lon * (dst.lon_max - dst.lon_min)
    mapped_lat = dst.lat_min + rel_lat * (dst.lat_max - dst.lat_min)
    return mapped_lon, mapped_lat


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

    The docs/.env assume repo-relative paths (e.g., raw/porto-trips/train.csv).
    When running from a subfolder (VS Code "Run Python File" often uses the
    file's directory as CWD), those paths break.

    Resolution order:
    1) As provided (relative to current working directory)
    2) Relative to repository root

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


def _porto_to_casablanca(lon: float, lat: float) -> Tuple[float, float]:
    """Linear bbox mapping (Task 04) Porto -> Casablanca, with relative-position clamping.

    This matches Notebook 03's ADR-01 behaviour (clamp rel coords to [0,1]) while
    keeping the capstone brief's fixed bboxes.
    """

    return _affine_bbox_map(lon, lat, _PORTO_BBOX, _CASABLANCA_BBOX)


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
        mapping_mode: str = "affine",
        clamp_bbox: bool = True,
        casa_place: str = "Casablanca, Morocco",
        casa_graphml: str = "/tmp/casablanca_drive.graphml",
        parquet_avg_speed_kmh: float = 25.0,
        parquet_max_pings: int = 400,
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

        self.mapping_mode = mapping_mode
        self.clamp_bbox = clamp_bbox
        self.casa_place = casa_place
        self.casa_graphml = casa_graphml
        self.parquet_avg_speed_kmh = parquet_avg_speed_kmh
        self.parquet_max_pings = parquet_max_pings

        self._road_ready = False
        self._ox = None
        self._nx = None
        self._G_casa = None
        self._G_undir = None

        self.producer = self._create_producer()
        self._delayed_sender = _DelayedKafkaSender(self.producer, self.topic)

    def _init_road_graph(self) -> None:
        if self._road_ready:
            return

        try:
            import networkx as nx  # type: ignore
            import osmnx as ox  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "Road map-matching mode requires optional deps. Install: pip install osmnx networkx"
            ) from exc

        ox.settings.log_console = False
        ox.settings.use_cache = True

        cache_path = Path(self.casa_graphml)
        if cache_path.exists():
            G = ox.load_graphml(cache_path)
            logger.info(
                "Loaded cached Casablanca road graph: %s (%d nodes, %d edges)",
                cache_path,
                G.number_of_nodes(),
                G.number_of_edges(),
            )
        else:
            logger.warning("Casablanca road graph cache not found; downloading via OSMnx: %s", self.casa_place)
            G = ox.graph_from_place(self.casa_place, network_type="drive", simplify=True)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            ox.save_graphml(G, cache_path)
            logger.info(
                "Saved Casablanca road graph cache: %s (%d nodes, %d edges)",
                cache_path,
                G.number_of_nodes(),
                G.number_of_edges(),
            )

        self._ox = ox
        self._nx = nx
        self._G_casa = G
        self._G_undir = G.to_undirected()
        self._road_ready = True

    def _extract_route_geometry(self, route: List[int]) -> List[Tuple[float, float]]:
        """Extract full edge geometry for a route (Notebook 03 ADR-03)."""

        if not route or len(route) < 2:
            return []
        if self._G_casa is None:
            return []

        G = self._G_casa
        coords: List[Tuple[float, float]] = []
        for i in range(len(route) - 1):
            u, v = route[i], route[i + 1]

            edge_data = G.get_edge_data(u, v)
            reverse = False
            if edge_data is None:
                edge_data = G.get_edge_data(v, u)
                reverse = True

            if edge_data is None:
                if i == 0:
                    coords.append((float(G.nodes[u].get("x")), float(G.nodes[u].get("y"))))
                coords.append((float(G.nodes[v].get("x")), float(G.nodes[v].get("y"))))
                continue

            key = next(iter(edge_data.keys()))
            edge = edge_data[key]
            geom = edge.get("geometry")
            if geom is not None:
                seg = list(geom.coords)
                if reverse:
                    seg = list(reversed(seg))
                else:
                    ux, uy = G.nodes[u].get("x"), G.nodes[u].get("y")
                    if ux is not None and uy is not None:
                        if abs(seg[0][0] - ux) + abs(seg[0][1] - uy) > 0.0001:
                            seg = list(reversed(seg))
                start = 0 if i == 0 else 1
                coords.extend([(float(x), float(y)) for x, y in seg[start:]])
            else:
                if i == 0:
                    coords.append((float(G.nodes[u].get("x")), float(G.nodes[u].get("y"))))
                coords.append((float(G.nodes[v].get("x")), float(G.nodes[v].get("y"))))

        return coords

    def _map_porto_trip_to_casa_road(self, porto_polyline: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
        """Map one Porto trip into Casablanca with road matching (Notebook 03 §4)."""

        if not porto_polyline or len(porto_polyline) < 2:
            return []

        # Desired ping count: preserve original 15s sampling density.
        target_pings = max(2, len(porto_polyline))

        start_lon, start_lat = porto_polyline[0]
        end_lon, end_lat = porto_polyline[-1]

        # Notebook 03 filter: skip round trips (≈100m). Here: fall back to affine-per-point.
        if abs(start_lon - end_lon) < 0.001 and abs(start_lat - end_lat) < 0.001:
            mapped = [_porto_to_casablanca(lon, lat) for lon, lat in porto_polyline]
            if self.clamp_bbox:
                mapped = [
                    _clamp_lon_lat_to_bbox(lon, lat, _CASABLANCA_BBOX)
                    for lon, lat in mapped
                ]
            return mapped

        try:
            self._init_road_graph()
        except Exception as exc:
            logger.warning("Road graph init failed; falling back to affine mapping (%s)", exc)
            mapped = [_porto_to_casablanca(lon, lat) for lon, lat in porto_polyline]
            if self.clamp_bbox:
                mapped = [
                    _clamp_lon_lat_to_bbox(lon, lat, _CASABLANCA_BBOX)
                    for lon, lat in mapped
                ]
            return mapped

        if self._G_casa is None or self._G_undir is None or self._ox is None or self._nx is None:
            return []

        # Transform O/D to Casablanca bbox.
        casa_start_lon, casa_start_lat = _porto_to_casablanca(start_lon, start_lat)
        casa_end_lon, casa_end_lat = _porto_to_casablanca(end_lon, end_lat)
        if self.clamp_bbox:
            casa_start_lon, casa_start_lat = _clamp_lon_lat_to_bbox(
                casa_start_lon, casa_start_lat, _CASABLANCA_BBOX
            )
            casa_end_lon, casa_end_lat = _clamp_lon_lat_to_bbox(
                casa_end_lon, casa_end_lat, _CASABLANCA_BBOX
            )

        try:
            origin = self._ox.distance.nearest_nodes(self._G_casa, X=casa_start_lon, Y=casa_start_lat)
            dest = self._ox.distance.nearest_nodes(self._G_casa, X=casa_end_lon, Y=casa_end_lat)
            if origin == dest:
                raise ValueError("origin==dest after snapping")

            route = self._nx.shortest_path(self._G_undir, origin, dest, weight="length")
            coords = self._extract_route_geometry(route)
            if len(coords) < 2:
                raise ValueError("degenerate route geometry")

            resampled = _resample_polyline(coords, target_pings)
            if self.clamp_bbox:
                resampled = [
                    _clamp_lon_lat_to_bbox(lon, lat, _CASABLANCA_BBOX)
                    for lon, lat in resampled
                ]
            return resampled
        except Exception as exc:
            logger.warning("Road map-matching failed; falling back to affine mapping (%s)", exc)
            mapped = [_porto_to_casablanca(lon, lat) for lon, lat in porto_polyline]
            if self.clamp_bbox:
                mapped = [
                    _clamp_lon_lat_to_bbox(lon, lat, _CASABLANCA_BBOX)
                    for lon, lat in mapped
                ]
            return mapped

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

        mode = (self.mapping_mode or "affine").strip().lower()
        if mode not in {"affine", "road"}:
            raise ValueError("mapping_mode must be 'affine' or 'road'")

        if mode == "road":
            logger.warning(
                "Mapping mode=road enabled: computing Casablanca road-matched routes (may be slow)."
            )

            # Best-effort: if deps/graph init aren't available, degrade to affine.
            try:
                self._init_road_graph()
            except Exception as exc:
                logger.warning("Road mode unavailable; falling back to affine mapping (%s)", exc)
                self._replay_trips(trips, is_map_matched=False)
                return

            mapped_trips: List[_Trip] = []
            for t in trips:
                mapped_polyline = self._map_porto_trip_to_casa_road(t.polyline)
                if len(mapped_polyline) < 2:
                    continue
                mapped_trips.append(
                    _Trip(
                        trip_id=t.trip_id,
                        taxi_id=t.taxi_id,
                        base_ts=t.base_ts,
                        polyline=mapped_polyline,
                    )
                )
            if not mapped_trips:
                raise ValueError("Road map-matching produced no valid trips")
            self._replay_trips(mapped_trips, is_map_matched=True)
        else:
            self._replay_trips(trips, is_map_matched=False)

    def _stream_from_parquet(self) -> None:
        logger.info("Loading parquet from %s...", self.data_path)
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

                # The curated dataset stores a dense road-geometry polyline.
                # Convert it into a realistic 15s ping series.
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

        self._replay_trips(trips, is_map_matched=True)

    def _replay_trips(self, trips: List[_Trip], is_map_matched: bool) -> None:
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
                    
                    if is_map_matched:
                        cas_lon, cas_lat = lon, lat
                    else:
                        cas_lon, cas_lat = _porto_to_casablanca(lon, lat)

                    # Speed within trip (15s per ping).
                    speed_kmh = 0.0
                    if point_idx > 0:
                        prev_lon, prev_lat = trip.polyline[point_idx - 1]
                        if is_map_matched:
                            prev_cas_lon, prev_cas_lat = prev_lon, prev_lat
                        else:
                            prev_cas_lon, prev_cas_lat = _porto_to_casablanca(prev_lon, prev_lat)
                        dist_km = _haversine_km(prev_cas_lon, prev_cas_lat, cas_lon, cas_lat)
                        speed_kmh = float((dist_km / 15.0) * 3600.0) if dist_km > 0 else 0.0

                    noisy_lat, noisy_lon = self._apply_gaussian_noise(cas_lat, cas_lon)

                    # For a live dashboard to work seamlessly with accelerated replay, 
                    # the emitted timestamp should match the current wall-clock time.
                    # sim_start + (event_ts - min_event_ts) / self.speed perfectly tracks the sleep loop.
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
                self._stream_from_porto_csv()
            else:
                if (self.mapping_mode or "").strip().lower() == "road":
                    logger.warning("mapping_mode=road ignored for parquet input; using parquet polyline replay")
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
        # default=os.environ.get("TAASIM_DATA_PATH", "raw/porto-trips/train.csv"),
        default=os.environ.get("TAASIM_DATA_PATH", "s3://taasim/raw/porto-trips/train.csv"),
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
        "--mapping-mode",
        default=os.environ.get("TAASIM_MAPPING_MODE", "affine"),
        choices=["affine", "road"],
        help="CSV input only: 'affine' (default) or 'road' (OSM map-matching).",
    )
    p.add_argument(
        "--casa-place",
        default=os.environ.get("TAASIM_CASA_PLACE", "Casablanca, Morocco"),
        help="OSMnx place string used when downloading the Casablanca road graph (road mode).",
    )
    p.add_argument(
        "--casa-graphml",
        default=os.environ.get("TAASIM_CASA_GRAPHML", "/tmp/casablanca_drive.graphml"),
        help="GraphML cache path for Casablanca road graph (road mode).",
    )
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
        help="Do not clamp emitted coordinates; may increase downstream invalid_bbox drops.",
    )
    p.set_defaults(clamp_bbox=True)
    p.add_argument(
        "--once",
        action="store_true",
        help="Run one pass through the input data then exit (CSV mode only).",
    )
    return p


if __name__ == "__main__":
    args = _build_arg_parser().parse_args()
    logger.warning("the building args are : %s" , args)
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
        mapping_mode=args.mapping_mode,
        clamp_bbox=args.clamp_bbox,
        casa_place=args.casa_place,
        casa_graphml=args.casa_graphml,
        parquet_avg_speed_kmh=args.parquet_avg_speed_kmh,
        parquet_max_pings=args.parquet_max_pings,
    )
    producer_service.start()

