# task04 — FastAPI Boilerplate & Zone Vehicles Endpoint

## Context
The FastAPI service is TaaSim's public interface — it receives trip reservations, exposes vehicle
availability, and serves ML forecasts. Bootstrapping it in Sprint 2 (before JWT auth in Sprint 5)
lets Founder B test the Cassandra data model live while Founder A develops the Flink jobs. The
`/vehicles/zone/{zone_id}` endpoint also acts as a quick correctness check: if it returns vehicles
with zone centroid coordinates, anonymisation is confirmed end-to-end.

## Objective
Bootstrap the FastAPI application and implement `GET /api/v1/vehicles/zone/{zone_id}` that reads
from Cassandra `vehicle_positions` and returns the list of active vehicles in the requested zone.

## Acceptance Criteria
- [ ] FastAPI app starts with `uvicorn api.main:app --host 0.0.0.0 --port 8000`
- [ ] `GET /api/v1/vehicles/zone/3` returns a JSON array of vehicle objects with fields:
  `taxi_id`, `lat`, `lon`, `status`, `event_time`
- [ ] Only vehicles with `event_time > now - 30 seconds` are returned (stale vehicles excluded)
- [ ] Returns `[]` (empty list) for a zone with no recent vehicles — not a 404 or 500
- [ ] `GET /docs` (Swagger UI) loads and shows the endpoint with request/response schema
- [ ] `POST /api/v1/trips` stub implemented: accepts `{origin_zone, destination_zone, rider_id}`,
  publishes a placeholder event to Kafka `raw.trips`, returns `{trip_id, status: "pending"}`
- [ ] Cassandra driver session initialised at startup (not per-request)

## Technical Hints
- Recommended Cassandra driver: `cassandra-driver` (DataStax):
  ```python
  from cassandra.cluster import Cluster
  cluster = Cluster(['cassandra'])
  session = cluster.connect('taasim')
  ```
- Initialise the session once using FastAPI's `lifespan` context manager (not deprecated
  `@app.on_event`):
  ```python
  from contextlib import asynccontextmanager
  @asynccontextmanager
  async def lifespan(app: FastAPI):
      app.state.session = cluster.connect('taasim')
      yield
      cluster.shutdown()
  app = FastAPI(lifespan=lifespan)
  ```
- For the Kafka publish in `/trips`, use `confluent-kafka` `Producer.produce()` then `flush()`.
- Use Pydantic models for request/response validation.
- Reference: project brief §9.5 FastAPI Service.

## Assigned To
Founder B

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
