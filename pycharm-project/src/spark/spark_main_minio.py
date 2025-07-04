from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Read Parquet from MinIO") \
    .config("spark.jars", "/home/ngocthanh/spark-3.5.3/jars/hadoop-aws-3.3.4.jar,/home/ngocthanh/spark-3.5.3/jars/aws-java-sdk-bundle-1.12.761.jar") \
    .getOrCreate()

# Configure MinIO connection
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "sOiuVZbAAGgmvx7Gm9c1")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "esMneNDLEzNoVz8mXrqgMl3GvOyfflD7nTBEm0Po")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "https://minio.vgpu.rdhasaki.com")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

# Read Parquet file from MinIO
bucket_name = "hasaki-datalake"
file_path = f"s3a://{bucket_name}/ga4-data/hasakiwork/analytics_253437596/events/2025/4/15/0.parquet"
df = spark.read.parquet(file_path)

# Show data
df.show()

# Stop SparkSession
spark.stop()