from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from config.spark_config import SparkConnect, get_spark_config
from src.spark.spark_write_database import SparkWriteDatabase


def main():
    jar_packages = [
        "com.clickhouse:clickhouse-jdbc:0.6.4",
        "org.apache.httpcomponents.client5:httpclient5:5.3.1",
    ]

    # Danh cho nhung config ma khong phai SparkSession nao cung su dung!
    spark_config = {
        "fs.s3a.access.key": "sOiuVZbAAGgmvx7Gm9c1",
        "fs.s3a.secret.key": "esMneNDLEzNoVz8mXrqgMl3GvOyfflD7nTBEm0Po",
        "fs.s3a.endpoint": "https://minio.vgpu.rdhasaki.com",
        "fs.s3a.path.style.access": "true",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.connection.ssl.enabled": "false",
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
        master_url="local[*]",
        # master_url="spark://spark-master:7077",
        executor_cores=2,
        executor_memory="1g",
        driver_memory="2g",
        num_executors=1,
        jar_packages=jar_packages,
        spark_conf=spark_config,
        log_level="WARN"
    )
    spark = spark_connect.spark

    # Read Parquet file from MinIO
    bucket_name = "hasaki-datalake"
    file_path = f"s3a://{bucket_name}/ga4-data/hasakiwork/analytics_253437596/events/2025/4/15/0.parquet"

    # Lược đồ đã được cập nhật dựa trên Parquet schema gốc
    parquet_schema = StructType([
        StructField("event_date", StringType(), True),
        StructField("event_timestamp", TimestampType(), True), # timestamp_ntz được ánh xạ thành TimestampType trong Spark
        StructField("event_name", StringType(), True),
        StructField("event_params", ArrayType(
            StructType([
                StructField("key", StringType(), True),
                StructField("value", StructType([
                    StructField("double_value", IntegerType(), True), # Giữ nguyên IntegerType theo Parquet gốc
                    StructField("float_value", IntegerType(), True),  # Giữ nguyên IntegerType theo Parquet gốc
                    StructField("int_value", LongType(), True),
                    StructField("string_value", StringType(), True)
                ]), True)
            ])
        ), True),
        StructField("event_previous_timestamp", LongType(), True),
        StructField("event_value_in_usd", DoubleType(), True),
        StructField("event_bundle_sequence_id", LongType(), True),
        StructField("event_server_timestamp_offset", LongType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_pseudo_id", StringType(), True),
        StructField("privacy_info", StructType([
            StructField("ads_storage", IntegerType(), True),      # Đã sửa: StringType -> IntegerType
            StructField("analytics_storage", IntegerType(), True),# Đã sửa: StringType -> IntegerType
            StructField("uses_transient_token", StringType(), True),
        ]), True),
        # Đã sửa: user_properties là Array of Integer, không phải Array of Struct
        StructField("user_properties", ArrayType(IntegerType(), containsNull=True), True),
        StructField("user_first_touch_timestamp", LongType(), True),
        StructField("user_ltv", StructType([
            StructField("currency", StringType(), True),
            StructField("revenue", DoubleType(), True),
        ]), True),
        StructField("device", StructType([
            StructField("advertising_id", IntegerType(), True),
            StructField("browser", IntegerType(), True),
            StructField("browser_version", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("is_limited_ad_tracking", StringType(), True),
            StructField("language", StringType(), True),
            StructField("mobile_brand_name", StringType(), True),
            StructField("mobile_marketing_name", StringType(), True),
            StructField("mobile_model_name", StringType(), True),
            StructField("mobile_os_hardware_model", StringType(), True),
            StructField("operating_system", StringType(), True),
            StructField("operating_system_version", StringType(), True),
            StructField("time_zone_offset_seconds", IntegerType(), True), # Đã sửa: LongType -> IntegerType
            StructField("vendor_id", IntegerType(), True),             # Đã sửa: StringType -> IntegerType
            # Đã sửa: web_info là StructType chi tiết
            StructField("web_info", StructType([
                StructField("browser", StringType(), True),
                StructField("browser_version", StringType(), True),
                StructField("hostname", StringType(), True),
            ]), True),
        ]), True),
        StructField("geo", StructType([
            StructField("city", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("country", StringType(), True),
            StructField("metro", StringType(), True),
            StructField("region", StringType(), True),
            StructField("sub_continent", StringType(), True),
        ]), True),
        StructField("app_info", IntegerType(), True), # Đã sửa: StructType -> IntegerType
        StructField("traffic_source", StructType([
            StructField("medium", StringType(), True),
            StructField("name", StringType(), True),
            StructField("source", StringType(), True),
        ]), True),
        StructField("stream_id", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("event_dimensions", IntegerType(), True),
        StructField("ecommerce", StructType([
            StructField("purchase_revenue", DoubleType(), True),
            StructField("purchase_revenue_in_usd", DoubleType(), True),
            StructField("refund_value", IntegerType(), True),
            StructField("refund_value_in_usd", IntegerType(), True),
            StructField("shipping_value", DoubleType(), True),
            StructField("shipping_value_in_usd", DoubleType(), True),
            StructField("tax_value", IntegerType(), True),
            StructField("tax_value_in_usd", IntegerType(), True),
            StructField("total_item_quantity", LongType(), True),
            StructField("transaction_id", StringType(), True), # Đã sửa: IntegerType -> StringType
            StructField("unique_items", LongType(), True),
        ]), True),
        StructField("items", ArrayType(
            StructType([
                StructField("affiliation", StringType(), True),
                StructField("coupon", StringType(), True),
                StructField("creative_name", StringType(), True),
                StructField("creative_slot", StringType(), True),
                StructField("item_brand", StringType(), True),
                StructField("item_category", StringType(), True),
                StructField("item_category2", StringType(), True),
                StructField("item_category3", StringType(), True),
                StructField("item_category4", StringType(), True),
                StructField("item_category5", StringType(), True),
                StructField("item_id", StringType(), True),
                StructField("item_list_id", StringType(), True),
                StructField("item_list_index", StringType(), True),
                StructField("item_list_name", StringType(), True),
                StructField("item_name", StringType(), True),
                StructField("item_params", ArrayType(IntegerType(), containsNull=True), True),
                StructField("item_refund", IntegerType(), True),
                StructField("item_refund_in_usd", IntegerType(), True),
                StructField("item_revenue", DoubleType(), True),      # Đã sửa: IntegerType -> DoubleType
                StructField("item_revenue_in_usd", DoubleType(), True), # Đã sửa: IntegerType -> DoubleType
                StructField("item_variant", StringType(), True),
                StructField("location_id", StringType(), True),
                StructField("price", DoubleType(), True),
                StructField("price_in_usd", DoubleType(), True),
                StructField("promotion_id", StringType(), True),
                StructField("promotion_name", StringType(), True),
                StructField("quantity", LongType(), True),
            ])
        ), True),
        StructField("collected_traffic_source", StructType([
            StructField("dclid", IntegerType(), True),
            StructField("gclid", StringType(), True),
            StructField("manual_campaign_id", StringType(), True), # Đã sửa: IntegerType -> StringType
            StructField("manual_campaign_name", StringType(), True),
            StructField("manual_content", StringType(), True),     # Đã sửa: IntegerType -> StringType
            StructField("manual_creative_format", IntegerType(), True),
            StructField("manual_marketing_tactic", IntegerType(), True),
            StructField("manual_medium", StringType(), True),
            StructField("manual_source", StringType(), True),
            StructField("manual_source_platform", IntegerType(), True),
            StructField("manual_term", StringType(), True),        # Đã sửa: IntegerType -> StringType
            StructField("srsltid", StringType(), True),            # Đã sửa: IntegerType -> StringType
        ]), True),
        StructField("is_active_user", BooleanType(), True),
        StructField("batch_event_index", LongType(), True),
        StructField("batch_page_id", LongType(), True),
        StructField("batch_ordering_id", LongType(), True),
        StructField("session_traffic_source_last_click", IntegerType(), True),
        StructField("publisher", IntegerType(), True),
    ])

    parquet_file = spark.read.schema(parquet_schema).parquet(file_path)

    # Biến đổi dữ liệu cho ClickHouse
    # Giữ nguyên các phép biến đổi to_json nếu các cột tương ứng trong ClickHouse là String/JSON
    # Nếu không, bạn sẽ cần điều chỉnh lại để khớp với kiểu dữ liệu Nested/Array(Tuple) của ClickHouse
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
        to_json(col("user_properties")).alias("user_properties"), # user_properties giờ là ArrayType(IntegerType), to_json sẽ thành "[1,2,3]"
        col("user_first_touch_timestamp"),
        to_json(col("user_ltv")).alias("user_ltv"),
        to_json(col("device")).alias("device"),
        to_json(col("geo")).alias("geo"),
        (col("app_info")).alias("app_info"), # Kieu du lieu la IntegerType, khong the dung to_json(), nếu ClickHouse cần String/JSON
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