import json
import os
from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.cassandra import CassandraSink
from pyflink.common import WatermarkStrategy

# --- 1. Define the Zone Lookup Logic ---
class CasablancaZoneMapper(MapFunction):
    def __init__(self):
        # We define your CSV data directly here for high performance in Task 01
        self.zones = [
            {"id": 1, "name": "Sidi Belyout", "lon": [-7.625, -7.595], "lat": [33.590, 33.620]},
            {"id": 2, "name": "Maarif", "lon": [-7.660, -7.615], "lat": [33.565, 33.595]},
            {"id": 3, "name": "Anfa", "lon": [-7.710, -7.660], "lat": [33.585, 33.630]},
            {"id": 4, "name": "Hay Hassani", "lon": [-7.730, -7.670], "lat": [33.530, 33.585]},
            {"id": 5, "name": "Mers Sultan", "lon": [-7.625, -7.595], "lat": [33.555, 33.590]},
            {"id": 6, "name": "Ain Chock", "lon": [-7.660, -7.610], "lat": [33.510, 33.565]},
            {"id": 7, "name": "Hay Mohammadi", "lon": [-7.595, -7.560], "lat": [33.585, 33.615]},
            {"id": 8, "name": "Sidi Bernoussi", "lon": [-7.540, -7.480], "lat": [33.600, 33.645]},
            {"id": 9, "name": "Ain Sebaa", "lon": [-7.560, -7.510], "lat": [33.595, 33.630]},
            {"id": 10, "name": "Roches Noires", "lon": [-7.595, -7.560], "lat": [33.605, 33.630]},
            {"id": 11, "name": "Sidi Moumen", "lon": [-7.540, -7.480], "lat": [33.560, 33.605]},
            {"id": 12, "name": "El Fida", "lon": [-7.615, -7.585], "lat": [33.565, 33.585]},
            {"id": 13, "name": "Mechouar", "lon": [-7.615, -7.600], "lat": [33.575, 33.590]},
            {"id": 14, "name": "Ben Msik", "lon": [-7.585, -7.550], "lat": [33.555, 33.580]},
            {"id": 15, "name": "Sbata", "lon": [-7.585, -7.540], "lat": [33.535, 33.565]},
            {"id": 16, "name": "Moulay Rachid", "lon": [-7.550, -7.500], "lat": [33.540, 33.580]}
        ]

    def map(self, value):
        event = json.loads(value)
        lon, lat = event['lon'], event['lat']
        speed = event.get('speed', 0)

        # 1. Validation: Speed Check
        if speed > 150: return None

        # 2. Validation: Zone Lookup & Anonymization
        for zone in self.zones:
            if (zone['lon'][0] <= lon <= zone['lon'][1]) and \
               (zone['lat'][0] <= lat <= zone['lat'][1]):
                
                # ENFORCE ANONYMIZATION: Replace raw with Centroid
                event['zone_id'] = zone['id']
                event['city'] = "Casablanca"
                event['lon'] = sum(zone['lon']) / 2
                event['lat'] = sum(zone['lat']) / 2
                return event
        
        return None # Filter out if not in any zone

def run_gps_normalizer():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # 1. Kafka Source
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("raw.gps") \
        .set_group_id("flink-job1-gps") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # 2. Apply Transformations
    normalized_stream = stream.map(CasablancaZoneMapper()).filter(lambda x: x is not None)

    # 3. Sink to Kafka (processed.gps)
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("processed.gps")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()
    
    normalized_stream.map(lambda x: json.dumps(x)).sink_to(kafka_sink)

    # 4. Sink to Cassandra (taasim.vehicle_positions)
    # We map the dictionary to a tuple that matches the Cassandra Table schema:
    # PRIMARY KEY ((city, zone_id), event_time)
    cassandra_stream = normalized_stream.map(lambda x: (
        x['city'], x['zone_id'], x['timestamp'], x['taxi_id'], 
        x['lat'], x['lon'], float(x['speed']), x['status']
    ))

    CassandraSink.add_sink(cassandra_stream) \
        .set_host("127.0.0.1") \
        .set_query("INSERT INTO taasim.vehicle_positions (city, zone_id, event_time, taxi_id, lat, lon, speed, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)") \
        .build()

    env.execute("TaaSim Job 1: GPS Normalizer")

if __name__ == '__main__':
    run_gps_normalizer()