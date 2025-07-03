from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from botocore.client import Config
import json


def create_minio_client():
    endpoint = 'https://minio.vgpu.rdhasaki.com'

    with open('/opt/airflow/credentials/credentials-minio.json') as credentials_file:
        data = json.load(credentials_file)
    
    access_key = data['accessKey']
    secret_key = data['secretKey']
    api_version = data['api']
    try:
        client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version=api_version),
            region_name='us-east-1'
        )
        return client
        
    except Exception as e:
        print(f"Failed when create minio client {str(e)}")

def explore_bucket_structure(**context):
    try:
        s3_client = create_minio_client()
        bucket_name = 'hasaki-datalake' # In file credentials
        
        # List all top-level directories
        print("\n TOP-LEVEL DIRECTORIES:")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/',
            MaxKeys=50
        )
        
        directories = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                dir_name = prefix['Prefix'].rstrip('/')
                directories.append(dir_name)
                print(f"{dir_name}")
        
        # Save directories for next task
        context['task_instance'].xcom_push(key='directories', value=directories)
        
        print(f"\n Found {len(directories)} directories")
        return directories
        
    except Exception as e:
        print(f"Error exploring bucket: {str(e)}")
        raise

def explore_ga4_data(**context):
    try:
        s3_client = create_minio_client()
        bucket_name = 'hasaki-datalake'
        ga4_prefix = 'ga4-data/hasakiwork/analytics_253437596/'
        
        # List all objects in GA4 path
        print(f"\n EXPLORING: {ga4_prefix}")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=ga4_prefix,
            MaxKeys=100
        )
        
        all_files = []
        total_size = 0
        file_types = {}
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Skip directories
                    if obj['Key'].endswith('/'):
                        continue
                        
                    file_info = {
                        'key': obj['Key'],
                        'filename': obj['Key'].split('/')[-1],
                        'size': obj['Size'],
                        'size_mb': obj['Size'] / (1024 * 1024),
                        'modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                        'extension': obj['Key'].split('.')[-1] if '.' in obj['Key'] else 'no_ext'
                    }
                    
                    all_files.append(file_info)
                    total_size += obj['Size']
                    
                    # Count file types
                    ext = file_info['extension']
                    file_types[ext] = file_types.get(ext, 0) + 1
        
        # Display results
        print(f"\n SUMMARY:")
        print(f"  Total files: {len(all_files)}")
        print(f"  Total size: {total_size / (1024 * 1024):.2f} MB")
        print(f"  File types: {file_types}")
        
        print(f"\n FILE LIST:")
        for i, file_info in enumerate(all_files[:20]):  # Show first 20 files
            print(f"  {i+1:2d}. {file_info['filename']}")
            print(f"      Size: {file_info['size_mb']:.2f} MB")
            print(f"      Modified: {file_info['modified']}")
            print(f"      Path: {file_info['key']}")
            print()
        
        if len(all_files) > 20:
            print(f"  ... and {len(all_files) - 20} more files")
        
        # Save file list for next task
        context['task_instance'].xcom_push(key='ga4_files', value=all_files)
        
        return all_files
        
    except Exception as e:
        print(f"Error exploring GA4 data: {str(e)}")
        raise

# Config DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 3),
    'retries': 0,
}

dag = DAG(
    'connect_to_minio',
    default_args=default_args,
    catchup=False
)

# Create tasks
task1 = PythonOperator(
    task_id='explore_bucket_structure',
    python_callable=explore_bucket_structure,
    dag=dag
)

task2 = PythonOperator(
    task_id='explore_ga4_data',
    python_callable=explore_ga4_data,
    dag=dag
)

# Workflow
task1 >> task2