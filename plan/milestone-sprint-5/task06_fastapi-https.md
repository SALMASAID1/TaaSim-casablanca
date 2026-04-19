# task06 — FastAPI HTTPS (Self‑Signed TLS for Demo)

## Context
The official course brief requires HTTPS on the API for the demo environment. A self‑signed
certificate is explicitly acceptable. The goal is to demonstrate that the API can be served over
TLS and that clients can connect (even if they must bypass certificate verification in dev).

## Objective
Enable HTTPS on the FastAPI service using Uvicorn TLS flags (`--ssl-keyfile`, `--ssl-certfile`).
Provide a repeatable way to generate a self‑signed certificate for local/demo use.

## Acceptance Criteria
- [ ] Self‑signed certificate generation is automated (script or documented command)
- [ ] FastAPI runs over HTTPS (not plain HTTP) in the demo setup
- [ ] `curl -k https://localhost:<port>/api/health` (or equivalent) succeeds
- [ ] HTTPS setup steps documented in `docs/api-https-setup.md`
- [ ] TLS private keys are **not committed** to Git (generate locally)

## Technical Hints
- Generate a dev cert locally:
  ```bash
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout infra/tls/dev.key -out infra/tls/dev.crt \
    -days 30 -subj "/CN=localhost"
  ```
- Run Uvicorn with TLS:
  ```bash
  uvicorn src.api.main:app --host 0.0.0.0 --port 8443 \
    --ssl-keyfile infra/tls/dev.key --ssl-certfile infra/tls/dev.crt
  ```
- For development testing: `curl -k` skips certificate verification.
- Reference: course brief §6.3 Security (HTTPS on API).

## Assigned To
Founder A

## Status
- [ ] Not started  
- [ ] In progress  
- [ ] Done  
- [ ] Blocked

## Notes / Blockers
_Free-form notes added during execution._
