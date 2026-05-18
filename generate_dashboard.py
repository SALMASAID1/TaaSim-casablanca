import json

ZONES_IN = "(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)"

def create_panel_geomap(id, title, x, y, h, w):
    query = (
        f"SELECT taxi_id, lat, lon, status, speed, event_time "
        f"FROM taasim.vehicle_positions "
        f"WHERE city='casablanca' AND zone_id IN {ZONES_IN} "
        f"AND event_time >= $__timeFrom AND event_time <= $__timeTo"
    )
    return {
        "id": id,
        "title": title,
        "type": "geomap",
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra"},
        "targets": [
            {
                "refId": "A",
                "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra"},
                "target": query,
                "rawQuery": True,
                "format": "table",
                "queryType": "query"
            }
        ],
        "fieldConfig": {
            "defaults": {
                "mappings": [
                    {
                        "type": "value",
                        "options": {
                            "available": {"color": "green", "index": 0, "text": "available"},
                            "assigned": {"color": "orange", "index": 1, "text": "assigned"},
                            "offline": {"color": "grey", "index": 2, "text": "offline"}
                        }
                    }
                ]
            }
        },
        "options": {
            "view": {
                "id": "coords",
                "lat": 33.589886,
                "lon": -7.603869,
                "zoom": 12
            },
            "basemap": {
                "type": "open-street-map"
            },
            "layers": [
                {
                    "type": "markers",
                    "name": "Vehicles",
                    "location": {
                        "mode": "latlon",
                        "latitude": "lat",
                        "longitude": "lon"
                    },
                    "color": {
                        "field": "status",
                        "mode": "fixed"
                    },
                    "size": {"fixed": 6},
                    "tooltip": True
                }
            ]
        }
    }

def create_panel_stat(id, title, x, y, h, w, query, color="green"):
    return {
        "id": id,
        "title": title,
        "type": "stat",
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra"},
        "targets": [
            {
                "refId": "A",
                "target": query,
                "rawQuery": True,
                "format": "table",
                "queryType": "query"
            }
        ],
        "options": {
            "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "area",
            "justifyMode": "auto"
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "fixed", "fixedColor": color},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [{"color": color, "value": None}]
                }
            }
        }
    }

def create_panel_bar(id, title, x, y, h, w, query):
    return {
        "id": id,
        "title": title,
        "type": "barchart",
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra"},
        "targets": [
            {
                "refId": "A",
                "target": query,
                "rawQuery": True,
                "format": "table",
                "queryType": "query"
            }
        ],
        "options": {
            "orientation": "horizontal",
            "barRadius": 0.5,
            "showValue": "always",
            "groupWidth": 0.7,
            "xField": "zone_id"
        }
    }

dashboard = {
    "title": "TaaSim Casablanca · Live Operations",
    "uid": "taasim-live",
    "refresh": "10s",
    "time": {"from": "now-30s", "to": "now"},
    "panels": [
        # Row 1: Fleet Overview
        create_panel_stat(2, "Active Vehicles", 0, 0, 4, 6, 
            f"SELECT count(taxi_id) FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id IN {ZONES_IN} AND event_time >= $__timeFrom AND event_time <= $__timeTo"),
        create_panel_stat(3, "Total Trips (Today)", 6, 0, 4, 6, 
            "SELECT count(trip_id) FROM taasim.trips WHERE city='casablanca' AND date_bucket = toDate(now()) ALLOW FILTERING", "blue"),
        create_panel_stat(4, "Avg Fare", 12, 0, 4, 6, 
            "SELECT avg(cast(fare as double)) FROM taasim.trips WHERE city='casablanca' AND date_bucket = toDate(now()) ALLOW FILTERING", "gold"),
        create_panel_stat(5, "Avg ETA (sec)", 18, 0, 4, 6, 
            "SELECT avg(cast(eta_seconds as double)) FROM taasim.trips WHERE city='casablanca' AND date_bucket = toDate(now()) ALLOW FILTERING", "purple"),

        # Row 2: Live Map
        create_panel_geomap(1, "Casablanca Fleet Real-Time Map", 0, 4, 12, 18),
        
        # Row 2 Side: Zone Distribution
        create_panel_bar(6, "Vehicles per Zone", 18, 4, 12, 6,
            f"SELECT cast(zone_id as text) as zone_id, count(taxi_id) FROM taasim.vehicle_positions WHERE city='casablanca' AND zone_id IN {ZONES_IN} AND event_time >= $__timeFrom AND event_time <= $__timeTo GROUP BY zone_id"),

        # Row 3: Market Dynamics
        {
            "id": 7,
            "title": "Zone Supply/Demand Heatmap (Sprint 3 Ready)",
            "type": "table",
            "gridPos": {"h": 8, "w": 24, "x": 0, "y": 16},
            "datasource": {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra"},
            "targets": [
                {
                    "refId": "A",
                    "target": f"SELECT zone_id, active_vehicles, pending_requests, ratio FROM taasim.demand_zones WHERE city='casablanca' AND zone_id IN {ZONES_IN} AND window_start >= $__timeFrom AND window_start <= $__timeTo",
                    "rawQuery": True,
                    "format": "table",
                    "queryType": "query"
                }
            ],
            "options": {
                "showHeader": True,
                "cellHeight": "sm"
            },
            "fieldConfig": {
                "defaults": {
                    "custom": {"align": "center"},
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "orange", "value": 0.5},
                            {"color": "green", "value": 1.0}
                        ]
                    }
                }
            }
        }
    ],
    "schemaVersion": 39
}

with open("grafana/dashboards/taasim-live.json", "w") as f:
    json.dump(dashboard, f, indent=2)

print("Successfully generated grafana/dashboards/taasim-live.json")
