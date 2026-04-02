import json
import time
import random
import pandas as pd
import logging
from kafka import KafkaProducer
from typing import Dict, Tuple, Optional

# Set up professional logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CasablancaGPSProducer:
    def __init__(self, broker: str, topic: str, data_path: str, speed: float = 10.0):
        self.broker = broker
        self.topic = topic
        self.data_path = data_path
        self.speed = speed
        
        # Simulation Constants
        self.DRIFT_METERS = 20.0
        self.BLACKOUT_CHANCE = 0.05
        self.BLACKOUT_DURATION = 60 # seconds
        
        # Producer Initialization
        self.producer = self._create_producer()
        self.blackout_registry: Dict[str, Tuple[int, int]] = {}

    def _create_producer(self) -> KafkaProducer:
        """Initializes the Kafka producer with optimized settings."""
        try:
            return KafkaProducer(
                bootstrap_servers=[self.broker],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1, # Wait for leader acknowledgment
                compression_type='gzip'
            )
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def _apply_gps_noise(self, lat: float, lon: float) -> Tuple[float, float]:
        """Calculates stochastic jitter based on real-world meter-to-degree conversion."""
        # 1 deg lat is ~111,320m. 1 deg lon at 33.5N is ~92,630m
        lat_drift = random.uniform(-self.DRIFT_METERS, self.DRIFT_METERS) / 111320.0
        lon_drift = random.uniform(-self.DRIFT_METERS, self.DRIFT_METERS) / 92630.0
        return round(lat + lat_drift, 6), round(lon + lon_drift, 6)

    def _register_blackouts(self, df: pd.DataFrame):
        """Pre-calculates which trips will experience connectivity loss."""
        unique_trips = df['TRIP_ID'].unique()
        for tid in unique_trips:
            if random.random() < self.BLACKOUT_CHANCE:
                trip_times = df[df['TRIP_ID'] == tid]['TIMESTAMP'].values
                if len(trip_times) > 5:
                    start_ts = random.choice(trip_times[:-5])
                    self.blackout_registry[tid] = (start_ts, start_ts + self.BLACKOUT_DURATION)
        logger.info(f"Blackout Engine Initialized: {len(self.blackout_registry)} trips affected.")

    def _is_in_blackout(self, trip_id: str, current_ts: int) -> bool:
        """Checks if the vehicle is currently in a 'tunnel' or 'dead zone'."""
        if trip_id in self.blackout_registry:
            start, end = self.blackout_registry[trip_id]
            return start <= current_ts <= end
        return False

    def start_simulation(self):
        """Main execution loop for the chronological replay."""
        logger.info(f"Loading data from {self.data_path}...")
        df = pd.read_parquet(self.data_path).sort_values(by="TIMESTAMP")
        
        self._register_blackouts(df)
        
        last_ts: Optional[int] = None
        logger.info(f"Broadcasting at {self.speed}x speed...")

        try:
            for _, row in df.iterrows():
                curr_ts = int(row['TIMESTAMP'])
                trip_id = str(row['TRIP_ID'])

                # 1. Connectivity Check
                if self._is_in_blackout(trip_id, curr_ts):
                    continue

                # 2. Temporal Alignment
                if last_ts is not None:
                    wait_time = (curr_ts - last_ts) / self.speed
                    if wait_time > 0:
                        time.sleep(wait_time)

                # 3. Spatial Jitter
                noise_lat, noise_lon = self._apply_gps_noise(row['cas_lat'], row['cas_lon'])

                # 4. Transmission
                payload = {
                    "timestamp": curr_ts,
                    "trip_id": trip_id,
                    "taxi_id": str(row['TAXI_ID']),
                    "lat": noise_lat,
                    "lon": noise_lon,
                    "arrondissement_id": int(row['arrondissement_id'])
                }
                
                self.producer.send(self.topic, value=payload)
                last_ts = curr_ts

        except KeyboardInterrupt:
            logger.info("Simulation paused by user.")
        finally:
            self.stop()

    def stop(self):
        """Graceful shutdown of Kafka resources."""
        logger.info("Flushing producer and closing connection...")
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    producer_service = CasablancaGPSProducer(
        broker='localhost:9092',
        topic='raw.gps',
        data_path='s3a://taasim/curated/casablanca_trips_final/',
        speed=10.0
    )
    producer_service.start_simulation()