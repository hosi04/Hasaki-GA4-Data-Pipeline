from datetime import datetime, timedelta, timezone
import os
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv

def create_minio_client():
    load_dotenv("/opt/airflow/.env")

    endpoint = os.getenv("ENDPOINT")
    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")

    try:
        client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        return client
    except Exception as e:
        print(f"Lỗi khi tạo MinIO client: {e}")
        raise

def detect_new_ga4_data(**context):
    s3_client = create_minio_client()
    bucket_name = os.getenv("BUCKET_NAME")
    ga4_prefix = 'ga4-data/hasakiwork/analytics_253437596/events/2025/7/15/'

    try:
        last_processed_timestamp_str = Variable.get("minio_ga4_last_processed_timestamp")
    except KeyError:
        last_processed_timestamp_str = "2000-01-01T00:00:00Z"
        Variable.set("minio_ga4_last_processed_timestamp", last_processed_timestamp_str)

    last_processed_dt = datetime.fromisoformat(last_processed_timestamp_str.replace('Z', '+00:00'))

    print(f"Bắt đầu kiểm tra các file mới hơn timestamp: {last_processed_dt.isoformat()}")

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=ga4_prefix,
        MaxKeys=1000
    )

    new_files_found = []
    current_latest_file_timestamp = last_processed_dt 

    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('/') or not obj['Key'].endswith('.parquet'):
                    continue
                
                file_modified_dt = obj['LastModified']
                
                if file_modified_dt > last_processed_dt:
                    new_files_found.append(obj['Key'])
                    if file_modified_dt > current_latest_file_timestamp:
                        current_latest_file_timestamp = file_modified_dt
                        print(f"--------------------------------------------------------{file_modified_dt}")

    if new_files_found:
        print(f"🎉 Tìm thấy {len(new_files_found)} file GA4 mới:")
        for f_path in new_files_found:
            print(f"- {f_path}")
        
        context['task_instance'].xcom_push(key='new_ga4_file_paths', value=new_files_found)
        context['task_instance'].xcom_push(key='detected_latest_timestamp', value=current_latest_file_timestamp.isoformat())
        
        return True
    else:
        print("💤 Không tìm thấy dữ liệu GA4 mới nào.")
        return False
    
def reset_last_processed_timestamp(**context):

    last_processed_timestamp_str = "2000-01-01T00:00:00Z"
    Variable.set("minio_ga4_last_processed_timestamp", last_processed_timestamp_str)

    print("🎉 Reset thanh cong")

def update_last_processed_timestamp(**context):
    detected_latest_timestamp = context['ti'].xcom_pull(task_ids='detect_new_ga4_data', key='detected_latest_timestamp')
    
    if detected_latest_timestamp:
        Variable.set("minio_ga4_last_processed_timestamp", detected_latest_timestamp)
    else:
        print("Không có timestamp mới nào để cập nhật (có thể không tìm thấy file mới).")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 7, 0, 0, 0, tzinfo=timezone.utc),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG(
    dag_id='minio_ga4_new_file_detector',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    detect_new_data_task = PythonOperator(
        task_id='detect_new_ga4_data',
        python_callable=detect_new_ga4_data,
        provide_context=True,
    )

    branch_on_new_data = BranchPythonOperator(
        task_id='branch_if_new_data',
        python_callable=lambda **context: 'spark_write_data' if context['ti'].xcom_pull(task_ids='detect_new_ga4_data') else 'no_new_data_message',
        provide_context=True,
    )

    spark_write_clickhouse = SparkSubmitOperator(
        task_id='spark_write_data',
        application="/opt/airflow/sparks/write_data_to_clickhouse.py",
        conn_id='spark-master',
        env_vars={ 
            'PYTHONPATH': '/opt/airflow/'
        },
        packages=(
            "com.clickhouse:clickhouse-jdbc:0.6.4,"
            "org.apache.httpcomponents.client5:httpclient5:5.3.1,"
            "org.apache.hadoop:hadoop-aws:3.3.5,"
            "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
        ),
        application_args=["{{ ti.xcom_pull(task_ids='detect_new_ga4_data', key='new_ga4_file_paths') | tojson }}"],    )

    update_variable_task = PythonOperator(
        task_id='update_last_processed_timestamp',
        python_callable=update_last_processed_timestamp,
        provide_context=True,
    )

    no_new_data_message = BashOperator(
        task_id='no_new_data_message',
        bash_command='echo "Không có dữ liệu GA4 mới để xử lý trong lần chạy này."',
    )

    detect_new_data_task >> branch_on_new_data
    branch_on_new_data >> spark_write_clickhouse >> update_variable_task
    branch_on_new_data >> no_new_data_message