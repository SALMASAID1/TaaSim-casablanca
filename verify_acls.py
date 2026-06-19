from kafka import KafkaProducer
import json
try:
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username="gps-producer",
        sasl_plain_password="gps-producer-secret"
    )
    future = producer.send("processed.demand", b"test_value")
    result = future.get(timeout=10)
    print("SUCCESS: Was able to write to processed.demand (THIS IS BAD)")
except Exception as e:
    print(f"FAILED (AS EXPECTED): Authorization error: {e}")
