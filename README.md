📦 GA4 Analytics Pipeline

📝 Project Description
This project builds a data pipeline to process Google Analytics 4 (GA4) event data stored in MinIO, loads it into ClickHouse, and transforms it into analytical data marts using dbt.

🚀 Features
- Scheduled GA4 data ingestion from MinIO using Apache Airflow
- Reads and flattens nested Parquet files with Apache Spark
- Writes transformed data into ClickHouse for high-performance querying
- Builds analytical data marts using dbt, orchestrated via Airflow
- Supports scalable, real-time reporting and BI use cases
