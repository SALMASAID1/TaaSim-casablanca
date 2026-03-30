# task01 — FastAPI JWT Authentication

## Context
TaaSim exposes trip reservation and demand forecast endpoints to the public internet. Without
authentication, anyone can query vehicle positions or spam the trip matching pipeline. The project
brief requires two roles: `rider` (read + reserve trips) and `admin` (full access including vehicle
positions and demand forecasts). JWT is the standard stateless auth mechanism for REST APIs.
This task hardens the API built in Sprint 2 before the final integration test in Sprint 6.

## Objective
Implement JWT-based authentication on all FastAPI endpoints using `python-jose`, with an
`/auth/token` login endpoint and two roles (`rider`, `admin`), enforced via `Depends()` guards.

## Acceptance Criteria
- [ ] `POST /auth/token` endpoint accepts `{username, password}` and returns `{access_token, token_type}`
- [ ] Tokens signed with HS256 and a secret key loaded from environment variable `JWT_SECRET`
- [ ] Token payload contains: `sub` (username), `role` (`rider` or `admin`), `exp` (expiry)
- [ ] Token expiry = 60 minutes
- [ ] **Rider role restrictions**: `GET /api/v1/vehicles/zone/{id}` returns HTTP 403 for rider tokens;
  `POST /api/v1/demand/forecast` returns HTTP 403 for rider tokens
- [ ] **Admin role**: full access to all endpoints
- [ ] All protected endpoints return HTTP 401 if no token provided
- [ ] All protected endpoints return HTTP 401 if token is expired or tampered
- [ ] `curl` test script `tests/test_jwt_auth.sh` committed, covering:
  - Valid rider token → can reserve trip ✓
  - Valid rider token → cannot access vehicles ✗ (403)
  - Valid admin token → can access all endpoints ✓
  - No token → 401
  - Expired token → 401

## Technical Hints
- Install: `pip install python-jose[cryptography] passlib[bcrypt]`
- Token creation:
  ```python
  from jose import jwt
  from datetime import datetime, timedelta

  def create_token(username: str, role: str) -> str:
      payload = {
          "sub": username,
          "role": role,
          "exp": datetime.utcnow() + timedelta(minutes=60)
      }
      return jwt.encode(payload, settings.JWT_SECRET, algorithm="HS256")
  ```
- Dependency for route protection:
  ```python
  from fastapi import Depends, HTTPException, status
  from fastapi.security import OAuth2PasswordBearer

  oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

  def require_admin(token: str = Depends(oauth2_scheme)):
      payload = jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
      if payload.get("role") != "admin":
          raise HTTPException(status_code=403, detail="Admin role required")
      return payload
  ```
- Use a hardcoded user dict for the demo (no DB needed): `{"admin": "adminpass", "rider1": "riderpass"}`.
- Reference: project brief §6.3 Security (API Authentication row), §9.5 FastAPI Service (JWT section).

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
