FROM apache/airflow:2.9.0-python3.11
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        librdkafka-dev \
        pkg-config \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt

# Install Airflow core & providers
RUN pip install --no-cache-dir \
    apache-airflow==2.9.0 \
    apache-airflow-providers-docker \
    apache-airflow-providers-ssh \
    boto3 \
    minio

# Install dbt-clickhouse (will auto install dbt-core)
RUN pip install --no-cache-dir dbt-clickhouse==1.9.2

# Optional: install other dependencies
RUN pip install --no-cache-dir \
    protobuf==3.20.3 \
    opentelemetry-proto