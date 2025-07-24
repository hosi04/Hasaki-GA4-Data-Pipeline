📦 GA4 Analytics Pipeline

📝 Project Description
This project builds a data pipeline to process Google Analytics 4 (GA4) event data stored in MinIO, loads it into ClickHouse, and transforms it into analytical data marts using dbt.

🚀 Features
- Scheduled GA4 data ingestion from MinIO using Apache Airflow
- Reads and flattens nested Parquet files with Apache Spark
- Writes transformed data into ClickHouse for high-performance querying
- Builds analytical data marts using dbt, orchestrated via Airflow
- Can be easily extended to support scalable, real-time Business Intelligence (BI) and dashboarding needs.

📂 Project Structure

project-root/\
├── airflow/\
│   └── dags/\
│       └── write_data_to_clickhouse_dag.py\
│
├── config/\
│   ├── database_config.py\
│   ├── minio_config.py\
│   └── spark_config.py\
│
├── sparks/\
│   ├── spark_write_database.py\
│   └── write_data_to_clickhouse.py\
│
├── .env.example\
├── .gitignore\
├── Dockerfile\
├── docker-compose.yaml\
├── README.md\
└── requirements.txt\
