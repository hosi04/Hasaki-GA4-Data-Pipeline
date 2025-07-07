import time

from pyspark.sql.functions import col, to_date, to_json
from pyspark.sql.types import *

from config.spark_config import SparkConnect, get_spark_config
from src.spark.spark_write_database import SparkWriteDatabase

def main():

    jar_packages = [
        "com.clickhouse:clickhouse-jdbc:0.6.4",
        "org.apache.httpcomponents.client5:httpclient5:5.3.1",
        # "org.apache.hadoop:hadoop-aws:3.3.4",
        # "com.amazonaws:aws-java-sdk-bundle:1.12.261"
    ]

    spark_connect = SparkConnect(
        app_name="thanhdz",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=3,
        jar_packages=jar_packages,
        log_level="WARN"
    )

    # Đọc file Parquet
    # parquet_file = spark_connect.spark.read.parquet("../../data/1.parquet")
    parquet_file = spark_connect.spark.read.parquet("/opt/airflow/spark_project/data/1.parquet")

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

    df_write = SparkWriteDatabase(spark_connect.spark, spark_config)
    df_write.spark_write_all_database(df_write_database, mode="append")

    # df_write.spark_read_database(spark_connect.spark, spark_config["clickhouse"]["table"], spark_config["clickhouse"]["jdbc_url"])
    spark_connect.stop()

if __name__ == "__main__":
    main()