# Sprint 1 — Evidence Pack

This folder contains the evidence artifacts for **Week 1 / Sprint 1** from the official brief.

## What’s inside

- `stack-health.png`, `stack-health.txt` — Docker Compose stack is running (Kafka, MinIO, Cassandra, Flink, Spark, Grafana, Kafka Connect)
- `minio-layout.md` — MinIO bucket structure (`raw/`, `curated/`, `ml/`, `raw/kafka-archive/`) + seeding proof
- `s3a-connector-setup.md` — Spark + Flink S3A connector configuration + read/write smoke tests
- `kafka-connect-s3-archive.md` — Kafka Connect S3 Sink configs + proof of objects written to `raw/kafka-archive/`
- `casablanca-coordinate-validation.png` — Porto→Casablanca coordinate remap validation plot
- `casablanca-interactive-locked.html` — interactive Folium map (locked bbox) used for visual validation

## Related notebook
The exploratory work and plots are produced in `notebooks/notebook-spark/01_data_exploration.ipynb`.
