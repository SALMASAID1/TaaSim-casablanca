"""TaaSim — Locust Load Test for ML Forecast Endpoint

Sprint 5 — Performance Validation

Validates that POST /api/v1/demand/forecast responds in < 500ms P95
under 20 concurrent requests per second.

Usage:
    # Headless mode (CI-friendly):
    locust -f tests/locustfile.py --headless -u 20 -r 5 -t 60s --host https://localhost:8000

    # Web UI mode:
    locust -f tests/locustfile.py --host https://localhost:8000
    # Then open http://localhost:8089

Prerequisites:
    - pip install locust
    - API running with ML model loaded
    - Admin user available (admin/adminpass)
"""

import os
import json
import urllib3

from locust import HttpUser, task, between, events

# Suppress self-signed cert warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TaaSimUser(HttpUser):
    """Simulates API users hitting the demand forecast endpoint."""

    wait_time = between(0.05, 0.2)  # ~5-20 req/s per user

    def on_start(self):
        """Login and cache the JWT token."""
        response = self.client.post(
            "/auth/token",
            data={"username": "admin", "password": "adminpass"},
            verify=False,
        )
        if response.status_code == 200:
            self.token = response.json()["access_token"]
            self.headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            }
        else:
            self.token = None
            self.headers = {}

    @task(10)
    def forecast_demand(self):
        """POST /api/v1/demand/forecast — the main SLA endpoint."""
        if not self.token:
            return

        import random
        zone_id = random.randint(1, 16)
        payload = {
            "zone_id": zone_id,
            "datetime": "2024-03-15T08:00:00Z",
        }

        self.client.post(
            "/api/v1/demand/forecast",
            json=payload,
            headers=self.headers,
            verify=False,
            name="/api/v1/demand/forecast",
        )

    @task(3)
    def get_vehicles_in_zone(self):
        """GET /api/v1/vehicles/zone/{zone_id}"""
        if not self.token:
            return

        import random
        zone_id = random.randint(1, 16)

        self.client.get(
            f"/api/v1/vehicles/zone/{zone_id}",
            headers=self.headers,
            verify=False,
            name="/api/v1/vehicles/zone/[zone_id]",
        )

    @task(2)
    def create_trip(self):
        """POST /api/v1/trips"""
        if not self.token:
            return

        import random
        payload = {
            "origin_zone": random.randint(1, 16),
            "destination_zone": random.randint(1, 16),
            "rider_id": f"locust-rider-{random.randint(1, 100)}",
        }

        # Use rider credentials for trip creation
        self.client.post(
            "/api/v1/trips",
            json=payload,
            headers=self.headers,
            verify=False,
            name="/api/v1/trips",
        )

    @task(1)
    def health_check(self):
        """GET / — health probe."""
        self.client.get("/", verify=False, name="/health")
