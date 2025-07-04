from typing import Dict
from pyspark.sql import DataFrame, SparkSession

class SparkWriteDatabase:
    def __init__(self, spark: SparkSession, spark_config: Dict):
        self.spark = spark
        self.spark_config = spark_config

    def spark_write_clickhouse(self, df: DataFrame, table_name: str, jdbc_url: str, mode: str = "append"):
        clickhouse_properties = {
            "user": "admin",
            "password": "admin",
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "batchsize": "100000",
            "socket_timeout": "300000",
            "rewriteBatchedStatements": "true",
            "numPartitions": "8",
            "jdbcCompliant": "false"
        }

        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("createTableOptions", "ENGINE = MergeTree() ORDER BY (event_date, event_name)") \
                .mode(mode) \
                .options(**clickhouse_properties) \
                .save()
            print("------------------------Dữ liệu đã được ghi thành công vào ClickHouse------------------------")
        except Exception as e:
            print(f"Lỗi khi ghi vào ClickHouse: {str(e)}")

    def spark_read_database(self, spark: SparkSession, table_name: str, jdbc_url: str):
        clickhouse_df = spark.read \
            .format("jdbc") \
            .option("url", f"{jdbc_url}") \
            .option("dbtable", f"(SELECT * FROM {table_name}) as sub_query") \
            .option("user", "admin") \
            .option("password", "admin") \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .load()
        clickhouse_df.show(truncate=False)

    def spark_write_all_database(self, df: DataFrame, mode: str = "append"):
        self.spark_write_clickhouse(
            df,
            self.spark_config["clickhouse"]["table"],
            self.spark_config["clickhouse"]["jdbc_url"],
            mode
        )
