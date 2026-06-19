import requests
import time
import numpy as np

url = "http://localhost:8000/auth/token"
resp = requests.post(url, data={"username": "admin", "password": "adminpass"})
token = resp.json()["access_token"]

url_forecast = "http://localhost:8000/api/v1/demand/forecast"
headers = {"Authorization": f"Bearer {token}"}
payload = {"zone_id": 5, "datetime": "2014-06-15T10:00:00Z"}

latencies = []
for i in range(20):
    start = time.time()
    resp_forecast = requests.post(url_forecast, headers=headers, json=payload)
    end = time.time()
    latencies.append((end - start) * 1000)

p95 = np.percentile(latencies, 95)
print(f"P95 Latency: {p95:.2f} ms")
print(f"Mean Latency: {np.mean(latencies):.2f} ms")
