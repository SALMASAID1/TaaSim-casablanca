"""TaaSim Casablanca — FastAPI Public Interface
==============================================

Endpoints
---------
GET  /                                   Health / readiness probe
POST /auth/token                         Issue a JWT access token for demo users
GET  /api/v1/vehicles/zone/{zone_id}     Latest vehicle positions in a zone (last 30 s)
POST /api/v1/trips                        Submit a trip request (publishes to Kafka raw.trips)

Architecture notes
------------------
* Cassandra session is initialised once per process via FastAPI's ``lifespan``
  context manager and stored on ``app.state``.  This avoids per-request
  connection overhead and keeps the driver's internal connection pool warm.

* The zone endpoint is partition-key-aligned:
    PK = (city, zone_id) → no ALLOW FILTERING required.
  ``event_time`` filtering is applied as a CQL inequality on the clustering
  column, which Cassandra can resolve as a fast range scan within the
  partition.

* The trip stub publishes a minimal event to ``raw.trips`` and returns a
  synthetic ``trip_id``.  Full trip-matching logic lives in the Flink Job 3
  pipeline (Sprint 3 scope).

Configuration (environment variables)
--------------------------------------
CASSANDRA_HOST      Cassandra broker hostname (default: cassandra)
CASSANDRA_PORT      Native CQL port         (default: 9042)
CASSANDRA_KEYSPACE  Keyspace                (default: taasim)
KAFKA_BROKER        Kafka bootstrap server  (default: kafka:29092)
KAFKA_TRIPS_TOPIC   Topic for trip events   (default: raw.trips)
JWT_SECRET          JWT signing secret      (default: taasim-dev-jwt-secret)
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, List, Literal, Optional

from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy, RoundRobinPolicy
from cassandra.query import SimpleStatement, ConsistencyLevel
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from kafka import KafkaProducer
from pydantic import BaseModel, Field
from jose import JWTError, jwt

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("taasim.api")

# ---------------------------------------------------------------------------
# Configuration (env → defaults)
# ---------------------------------------------------------------------------
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "taasim")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
KAFKA_TRIPS_TOPIC = os.getenv("KAFKA_TRIPS_TOPIC", "raw.trips")
JWT_SECRET = os.getenv("JWT_SECRET", "taasim-dev-jwt-secret")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

DEMO_USERS = {
    "admin": {"password": "adminpass", "role": "admin"},
    "rider1": {"password": "riderpass", "role": "rider"},
}

# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------


class VehiclePosition(BaseModel):
    taxi_id: str
    lat: float
    lon: float
    status: str
    event_time: datetime


class TripRequest(BaseModel):
    origin_zone: int = Field(..., ge=1, description="Origin zone ID (1-16)")
    destination_zone: int = Field(..., ge=1, description="Destination zone ID (1-16)")
    rider_id: str = Field(..., min_length=1)


class TripResponse(BaseModel):
    trip_id: str
    status: str = "pending"





class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class AuthenticatedUser(BaseModel):
    username: str
    role: Literal["rider", "admin"]


def authenticate_user(username: str, password: str) -> Optional[AuthenticatedUser]:
    user = DEMO_USERS.get(username)
    if user is None or user["password"] != password:
        return None
    return AuthenticatedUser(username=username, role=user["role"])


def create_access_token(username: str, role: str) -> str:
    payload = {
        "sub": username,
        "role": role,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _invalid_token() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )


def get_current_user(token: str = Depends(oauth2_scheme)) -> AuthenticatedUser:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError as exc:
        raise _invalid_token() from exc

    username = payload.get("sub")
    role = payload.get("role")
    if not isinstance(username, str) or role not in {"rider", "admin"}:
        raise _invalid_token()

    user = DEMO_USERS.get(username)
    if user is None or user["role"] != role:
        raise _invalid_token()

    return AuthenticatedUser(username=username, role=role)


def require_authenticated_user(current_user: AuthenticatedUser = Depends(get_current_user)) -> AuthenticatedUser:
    return current_user


def require_admin(current_user: AuthenticatedUser = Depends(get_current_user)) -> AuthenticatedUser:
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required",
        )
    return current_user


# ---------------------------------------------------------------------------
# Cassandra helper
# ---------------------------------------------------------------------------

ZONE_QUERY = SimpleStatement(
    """
    SELECT taxi_id, lat, lon, status, event_time
    FROM taasim.vehicle_positions
    WHERE city = 'casablanca'
      AND zone_id = %(zone_id)s
      AND event_time >= %(cutoff)s
    """,
    consistency_level=ConsistencyLevel.LOCAL_ONE,
)


def _connect_cassandra(max_attempts: int = 12, delay_s: float = 5.0) -> Session:
    """Create a Cassandra cluster connection with retries + backoff.

    Retries up to *max_attempts* times, sleeping *delay_s* seconds between each
    attempt.  This tolerates the race between the ``cassandra-init`` container
    applying the schema and the API container starting up.
    """
    import time as _time

    last_exc: Exception = RuntimeError("No connection attempts made")

    for attempt in range(1, max_attempts + 1):
        try:
            cluster = Cluster(
                contact_points=[CASSANDRA_HOST],
                port=CASSANDRA_PORT,
                load_balancing_policy=RoundRobinPolicy(),
                connect_timeout=60,
                protocol_version=4,
            )
            # Connect without keyspace first, then set it — more robust when
            # the keyspace may not exist yet on the very first start.
            session = cluster.connect()
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} "
                f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
            )
            session.set_keyspace(CASSANDRA_KEYSPACE)
            logger.info(
                "Cassandra session established → %s:%d / keyspace=%s (attempt %d/%d)",
                CASSANDRA_HOST, CASSANDRA_PORT, CASSANDRA_KEYSPACE, attempt, max_attempts,
            )
            return session
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Cassandra connection attempt %d/%d failed: %s — retrying in %.0fs…",
                attempt, max_attempts, exc, delay_s,
            )
            if attempt < max_attempts:
                _time.sleep(delay_s)

    raise RuntimeError(
        f"Cassandra unavailable after {max_attempts} attempts"
    ) from last_exc


def _connect_kafka() -> Optional[KafkaProducer]:
    """Create a Kafka producer.  Returns None if Kafka is unreachable (non-fatal for dev)."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
            acks=1,
            retries=5,
            linger_ms=10,
            client_id="taasim-api",
        )
        logger.info("Kafka producer ready → %s (topic: %s)", KAFKA_BROKER, KAFKA_TRIPS_TOPIC)
        return producer
    except Exception as exc:
        logger.warning("Kafka producer unavailable (%s). /trips stub will error.", exc)
        return None


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialise shared resources before the server starts accepting requests."""
    logger.info("Starting TaaSim API…")

    # Cassandra — required
    app.state.cassandra: Session = _connect_cassandra()

    # Kafka — optional (log warning only in dev if not reachable)
    app.state.kafka: Optional[KafkaProducer] = _connect_kafka()

    yield  # ← server is live here

    # ── Shutdown ──────────────────────────────────────────────────────────
    logger.info("Shutting down TaaSim API…")
    if app.state.kafka:
        try:
            app.state.kafka.flush(timeout=10)
            app.state.kafka.close(timeout=10)
        except Exception:
            pass
    if app.state.cassandra:
        try:
            app.state.cassandra.cluster.shutdown()
        except Exception:
            pass
    logger.info("TaaSim API stopped.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="TaaSim Casablanca API",
    description=(
        "Real-time fleet management API for the TaaSim Casablanca simulation. "
        "Backed by Apache Cassandra for low-latency position reads and Apache "
        "Kafka for trip-request event publishing."
    ),
    version="0.2.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/", tags=["health"])
async def root() -> dict:
    """Readiness probe — returns service name and OK status."""
    return {"service": "TaaSim API", "version": "0.2.0", "status": "ok"}


@app.post(
    "/auth/token",
    response_model=TokenResponse,
    tags=["auth"],
    summary="Issue a JWT access token for the demo users",
)
async def issue_token(form_data: OAuth2PasswordRequestForm = Depends()) -> TokenResponse:
    user = authenticate_user(form_data.username, form_data.password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return TokenResponse(access_token=create_access_token(user.username, user.role))


@app.get(
    "/api/v1/vehicles/zone/{zone_id}",
    response_model=List[VehiclePosition],
    tags=["vehicles"],
    summary="Latest vehicle positions in a zone (last 30 s)",
)
async def vehicles_in_zone(
    zone_id: int,
    request: Request,
    current_user: AuthenticatedUser = Depends(require_admin),
) -> List[VehiclePosition]:
    """Return all vehicle positions in *zone_id* reported within the last 30 seconds.

    **Query strategy**: The Cassandra table is partitioned on ``(city, zone_id)``
    and clustered on ``event_time DESC``.  This endpoint stays fully partition-aligned
    — no ``ALLOW FILTERING`` — by providing both partition-key components and a
    clustering-column range predicate.
    """
    if zone_id < 1 or zone_id > 16:
        raise HTTPException(status_code=422, detail="zone_id must be between 1 and 16")

    session: Session = request.app.state.cassandra
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=30)

    try:
        rows = session.execute(
            ZONE_QUERY,
            parameters={"zone_id": zone_id, "cutoff": cutoff},
        )
    except Exception as exc:
        logger.error("Cassandra query failed for zone %d: %s", zone_id, exc)
        raise HTTPException(status_code=503, detail="Cassandra query failed") from exc

    results: List[VehiclePosition] = []
    for row in rows:
        # event_time from Cassandra is already a datetime (timezone-aware or naive).
        et = row.event_time
        if et is not None and et.tzinfo is None:
            et = et.replace(tzinfo=timezone.utc)
        results.append(
            VehiclePosition(
                taxi_id=row.taxi_id,
                lat=row.lat,
                lon=row.lon,
                status=row.status or "unknown",
                event_time=et or datetime.now(timezone.utc),
            )
        )

    logger.info(
        "zone=%d requested by=%s → %d vehicles in last 30s",
        zone_id,
        current_user.username,
        len(results),
    )
    return results


@app.post(
    "/api/v1/trips",
    response_model=TripResponse,
    status_code=202,
    tags=["trips"],
    summary="Submit a trip request (stub — publishes to raw.trips)",
)
async def create_trip(
    body: TripRequest,
    request: Request,
    current_user: AuthenticatedUser = Depends(require_authenticated_user),
) -> TripResponse:
    """Publish a trip-request event to Kafka topic ``raw.trips``.

    This is a **stub**: the trip_id is generated here and the event is
    forwarded to Kafka for downstream Flink processing (Sprint 3).
    The response is synchronous but the full match is asynchronous.
    """
    kafka: Optional[KafkaProducer] = request.app.state.kafka
    if kafka is None:
        raise HTTPException(status_code=503, detail="Kafka producer is not available")

    trip_id = str(uuid.uuid4())
    event = {
        "trip_id": trip_id,
        "rider_id": body.rider_id,
        "origin_zone": body.origin_zone,
        "destination_zone": body.destination_zone,
        "requested_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "api",
    }

    try:
        future = kafka.send(KAFKA_TRIPS_TOPIC, key=trip_id, value=event)

        def _on_error(exc: BaseException) -> None:
            logger.error("Kafka send failed for trip_id=%s: %s", trip_id, exc)

        future.add_errback(_on_error)
    except Exception as exc:
        logger.error("Failed to enqueue trip %s: %s", trip_id, exc)
        raise HTTPException(status_code=503, detail="Failed to publish trip event") from exc

    logger.info(
        "Trip enqueued → trip_id=%s origin=%d dest=%d rider=%s requested_by=%s",
        trip_id,
        body.origin_zone,
        body.destination_zone,
        body.rider_id,
        current_user.username,
    )

    return TripResponse(trip_id=trip_id, status="pending")


@app.post(
    "/api/v1/demand/forecast",
    status_code=202,
    tags=["demand"],
    summary="Request demand forecast (stub)",
)
async def get_demand_forecast(
    request: Request,
    current_user: AuthenticatedUser = Depends(require_admin),
) -> dict:
    """Stub for demand forecast ML endpoint. Returns 202 Accepted.
    
    Restricted to admin users only.
    """
    return {"status": "accepted", "message": "Forecast computation started"}
