from pyspark.sql.functions import *
from pyspark.sql.types import *
from config.minio_config import get_minio_config
from config.spark_config import SparkConnect, get_spark_config
from spark_write_database import SparkWriteDatabase
import json
import sys
from functools import reduce
from pyspark.sql import DataFrame

def main():
    jar_packages = [
        "com.clickhouse:clickhouse-jdbc:0.6.4",
        "org.apache.httpcomponents.client5:httpclient5:5.3.1",
    ]
    minio_config = get_minio_config()

    spark_config = {
        "fs.s3a.access.key": "{}".format(minio_config["minio"].access_key),
        "fs.s3a.secret.key": "{}".format(minio_config["minio"].secret_key),
        "fs.s3a.endpoint": "{}".format(minio_config["minio"].endpoint),
        "fs.s3a.path.style.access": "true",
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.threads.keepalivetime": "60000",
        "fs.s3a.connection.establish.timeout": "30000",
        "fs.s3a.connection.timeout": "200000",
        "fs.s3a.socket.timeout": "200000",
        "fs.s3a.connection.acquire.timeout": "60000",
        "fs.s3a.multipart.purge.age": "86400000",
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.sql.mapKeyDedupPolicy": "LAST_WIN"
    }

    spark_connect = SparkConnect(
        app_name="thanhdz",
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
        sys.exit(1)

    if not new_file_paths:
        print("Không có file mới nào được truyền để xử lý. Kết thúc Spark job.")
        spark.stop()
        return

    parquet_schema = StructType([
        StructField("event_date", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("event_name", StringType(), True),
        StructField("event_params", ArrayType(
            StructType([
                StructField("key", StringType(), True),
                StructField("value", StructType([
                    StructField("double_value", IntegerType(), True),
                    StructField("float_value", IntegerType(), True),
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
            StructField("ads_storage", IntegerType(), True),
            StructField("analytics_storage", IntegerType(), True),
            StructField("uses_transient_token", StringType(), True),
        ]), True),
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
            StructField("time_zone_offset_seconds", IntegerType(), True),
            StructField("vendor_id", IntegerType(), True),
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
        StructField("app_info", IntegerType(), True),
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
            StructField("transaction_id", StringType(), True),
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
                StructField("item_revenue", DoubleType(), True),
                StructField("item_revenue_in_usd", DoubleType(), True),
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
            StructField("manual_campaign_id", StringType(), True),
            StructField("manual_campaign_name", StringType(), True),
            StructField("manual_content", StringType(), True),
            StructField("manual_creative_format", IntegerType(), True),
            StructField("manual_marketing_tactic", IntegerType(), True),
            StructField("manual_medium", StringType(), True),
            StructField("manual_source", StringType(), True),
            StructField("manual_source_platform", IntegerType(), True),
            StructField("manual_term", StringType(), True),
            StructField("srsltid", StringType(), True),
        ]), True),
        StructField("is_active_user", BooleanType(), True),
        StructField("batch_event_index", LongType(), True),
        StructField("batch_page_id", LongType(), True),
        StructField("batch_ordering_id", LongType(), True),
        StructField("session_traffic_source_last_click", IntegerType(), True),
        StructField("publisher", IntegerType(), True),
    ])

    bucket_name = "{}".format(minio_config["minio"].bucket_name)
    all_full_s3a_paths = [f"s3a://{bucket_name}/{path}" for path in new_file_paths]

    skipped_files = []

    spark_config = get_spark_config()
    for file_path in all_full_s3a_paths:
        print(f"Đang xử lý file: {file_path}")
        try:
            df_single_file = spark.read.schema(parquet_schema).parquet(file_path)

            df_write_database = df_single_file.select(
                to_date(col("event_date"), "yyyyMMdd").alias("event_date"),
                col("event_timestamp").cast("timestamp").alias("event_timestamp"),
                col("event_name"),
                # Transform event_params sử dụng SQL expression
                expr("""
                            to_json(
                                map_from_arrays(
                                    transform(event_params, x -> x.key),
                                    transform(event_params, x -> 
                                        case 
                                            when x.value.string_value is not null then cast(x.value.string_value as string)
                                            when x.value.int_value is not null then cast(x.value.int_value as string)
                                            when x.value.float_value is not null then cast(x.value.float_value as string)
                                            when x.value.double_value is not null then cast(x.value.double_value as string)
                                            else cast(x.value as string)
                                        end
                                    )
                                )
                            )
                    """).alias("event_params"),
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
            df_write = SparkWriteDatabase(spark, spark_config)
            df_write.spark_write_all_database(df_write_database, mode="append")

        except Exception as e:
            print(f"❌Lỗi khi đọc file '{file_path}': {str(e)}")
            skipped_files.append(file_path)
            continue


    spark_connect.stop()

if __name__ == "__main__":
    main()