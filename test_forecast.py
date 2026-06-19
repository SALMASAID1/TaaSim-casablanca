import requests
import time

url = "http://localhost:8000/auth/token"
resp = requests.post(url, data={"username": "admin", "password": "adminpass"})
token = resp.json()["access_token"]

url_forecast = "http://localhost:8000/api/v1/demand/forecast"
headers = {"Authorization": f"Bearer {token}"}
payload = {"zone_id": 5, "datetime": "2014-06-15T10:00:00Z"}

start = time.time()
resp_forecast = requests.post(url_forecast, headers=headers, json=payload)
end = time.time()

print(f"Status Code: {resp_forecast.status_code}")
print(f"Response: {resp_forecast.json()}")
print(f"Latency: {(end - start) * 1000:.2f} ms")
