# 📦 GA4 Analytics Pipeline

## 📝 Project Description
This project builds a data pipeline to process Google Analytics 4 (GA4) event data stored in MinIO, loads it into ClickHouse, and transforms it into analytical data marts using dbt.

## 🚀 Features
- Scheduled GA4 data ingestion from MinIO using Apache Airflow
- Reads and flattens nested Parquet files with Apache Spark
- Writes transformed data into ClickHouse for high-performance querying
- Builds analytical data marts using dbt, orchestrated via Airflow
- Can be easily extended to support scalable, real-time Business Intelligence (BI) and dashboarding needs.

## 📂 Project Structure

project-root/ \
├── airflow/ \
│   └── dags/ \
│       └── write_data_to_clickhouse_dag.py \
│
├── config/ \
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
└── requirements.txt

## 📁 Folder Explanation

| Folder/File           | Description |
|------------------------|-------------|
| `airflow/dags/`        | Contains DAG to orchestrate the pipeline |
| `config/`              | Configuration for Spark, MinIO, and ClickHouse |
| `sparks/`              | Spark ETL jobs for flattening and writing data |
| `.env.example`         | Environment variable example |
| `docker-compose.yaml`  | Spins up Airflow, MinIO, ClickHouse, etc. |


## 🛠️ Setup Instructions

⚠️ Make sure you have docker installed.
#### 1. Clone the Repository
```https://github.com/hosi04/Hasaki-GA4-Data-Pipeline.git```

#### 2. Docker Setup
    Step 1: docker-compose build

    Step 2: docker-compose up -d

## 📊 Data Flow

MinIO (Contain Parquet file) \
   ↓ \
Apache Airflow (DAG trigger) \
   ↓ \
Apache Spark (Read, transform, write) \
   ↓ \
ClickHouse (Store raw data) \
   ↓ \
dbt (Build data marts)


## ✍️ Author
- Gmail: hosinguyenn@gmail.com
- Phone: 0395612573
- Linkedin: https://tinyurl.com/hosi04 
- Github: https://github.com/hosi04

