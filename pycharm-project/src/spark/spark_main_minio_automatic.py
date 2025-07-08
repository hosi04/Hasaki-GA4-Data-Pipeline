from pyspark.sql.functions import *
from config.spark_config import SparkConnect, get_spark_config
from src.spark.spark_write_database import SparkWriteDatabase
import json
import sys

def main():
    jar_packages = [
        "com.clickhouse:clickhouse-jdbc:0.6.4",
        "org.apache.httpcomponents.client5:httpclient5:5.3.1",
    ]

    # Danh cho nhung config ma khong phai SparkSession nao cung su dung!
    spark_config = {
        "fs.s3a.access.key": "minioadmin",
        "fs.s3a.secret.key": "minioadmin123",
        "fs.s3a.endpoint": "http://minio:9000",
        "fs.s3a.path.style.access": "true",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # "fs.s3a.connection.ssl.enabled": "false",
        # Error XXs
        "fs.s3a.threads.keepalivetime": "60000",
        "fs.s3a.connection.establish.timeout": "30000",
        "fs.s3a.connection.timeout": "200000",
        "fs.s3a.socket.timeout": "200000",
        "fs.s3a.connection.acquire.timeout": "60000",
        "fs.s3a.multipart.purge.age": "86400000",
        # Thêm dòng này để fix lỗi credential provider
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }

    spark_connect = SparkConnect(
        app_name="thanhdz",
        # master_url="local[*]",
        master_url="spark://spark-master:7077",
        executor_cores=2,
        executor_memory="1g",
        driver_memory="2g",
        num_executors=1,
        jar_packages=jar_packages,
        spark_conf=spark_config,
        log_level="WARN"
    )
    spark = spark_connect.spark

    new_file_paths_json = sys.argv[1] if len(sys.argv) > 1 else "[]"

    try:
        new_file_paths = json.loads(new_file_paths_json)
    except json.JSONDecodeError:
        print(f"Lỗi: Không thể phân tích chuỗi JSON đường dẫn file: {new_file_paths_json}")
        spark.stop()
        sys.exit(1)  # Thoát với mã lỗi nếu không phân tích được JSON

    if not new_file_paths:
        print("Không có file mới nào được truyền để xử lý. Kết thúc Spark job.")
        spark.stop()
        return  # Thoát hàm main nếu không có file nào

    # Chuyển đổi danh sách đường dẫn tương đối thành đường dẫn S3A đầy đủ
    bucket_name = "hasaki-datalake"  # Tên bucket của bạn
    full_s3a_paths = [f"s3a://{bucket_name}/{path}" for path in new_file_paths]

    print(f"Đang đọc dữ liệu từ các đường dẫn MinIO sau: {full_s3a_paths}")

    # Read Parquet file from MinIO
    parquet_file = spark.read.parquet(*full_s3a_paths)

    # Biến đổi dữ liệu cho ClickHouse
    df_write_database = parquet_file.select(
        to_date(col("event_date"), "yyyyMMdd").alias("event_date"),
        col("event_timestamp").cast("timestamp").alias("event_timestamp"),
        col("event_name"),
        to_json(col("event_params")).alias("event_params"),
        col("event_previous_timestamp"),
        col("event_value_in_usd").alias("event_value_in_usd"),
        col("event_bundle_sequence_id"),
        col("event_server_timestamp_offset"),
        col("user_id"),
        col("user_pseudo_id"),
        to_json(col("privacy_info")).alias("privacy_info"),
        to_json(col("user_properties")).alias("user_properties"),
        col("user_first_touch_timestamp"),
        to_json(col("user_ltv")).alias("user_ltv"),
        to_json(col("device")).alias("device"),
        to_json(col("geo")).alias("geo"),
        col("app_info").alias("app_info"),
        to_json(col("traffic_source")).alias("traffic_source"),
        col("stream_id"),
        col("platform"),
        col("event_dimensions"),
        to_json(col("ecommerce")).alias("ecommerce"),
        to_json(col("items")).alias("items"),
        to_json(col("collected_traffic_source")).alias("collected_traffic_source"),
        col("is_active_user"),
        col("batch_event_index"),
        col("batch_page_id"),
        col("batch_ordering_id"),
        col("session_traffic_source_last_click"),
        col("publisher")
    )

    spark_config = get_spark_config()
    df_write = SparkWriteDatabase(spark, spark_config)
    df_write.spark_write_all_database(df_write_database, mode="append")

    spark_connect.stop()

if __name__ == "__main__":
    main()