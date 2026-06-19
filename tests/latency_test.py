import requests
import time
import numpy as np

url = "https://localhost:8000/auth/token"
resp = requests.post(url, data={"username": "admin", "password": "adminpass"}, verify=False)
token = resp.json()["access_token"]

url_forecast = "https://localhost:8000/api/v1/vehicles/zone/5"
headers = {"Authorization": f"Bearer {token}"}

latencies = []
for i in range(20):
    start = time.time()
    resp_forecast = requests.get(url_forecast, headers=headers, verify=False)
    end = time.time()
    latencies.append((end - start) * 1000)

p95 = np.percentile(latencies, 95)
print(f"P95 Latency: {p95:.2f} ms")
print(f"Mean Latency: {np.mean(latencies):.2f} ms")
