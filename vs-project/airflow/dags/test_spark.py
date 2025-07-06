from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 3),
    'retries': 0,
}

with DAG(
    dag_id='my_spark_inline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    spark_write_clickhouse = SparkSubmitOperator(
        task_id='spark_write_data',
        application="/opt/airflow/spark_project/src/spark/spark_main.py",
        conn_id='spark_default',
        conf={
            'spark.master': 'local[*]',
        },
        env_vars={
            'PYTHONPATH': '/opt/airflow/spark_project'
        },
        packages="com.clickhouse:clickhouse-jdbc:0.6.4,org.apache.httpcomponents.client5:httpclient5:5.3.1",
    )

    spark_write_clickhouse
