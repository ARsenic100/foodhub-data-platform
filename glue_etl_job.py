import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

sc = SparkContext()
spark = SparkSession(sc)

# Read from S3 raw orders
df = spark.read.csv(
    "s3://foodhub-datalake-team3/raw/orders/",
    header=True,
    inferSchema=True
)

# 🔹 Drop null order_id
df = df.dropna(subset=["order_id"])

# 🔹 Cast columns
df = df.withColumn("total_amount", col("total_amount").cast("double"))

# If these exist:
# df = df.withColumn("total_fees", col("total_fees").cast("double"))
# df = df.withColumn("total_fare", col("total_fare").cast("double"))

# 🔹 Convert date
df = df.withColumn("order_date", col("order_date").cast("date"))

# 🔹 Add ingestion timestamp
df = df.withColumn("ingestion_timestamp", current_timestamp())

# 🔹 Write as Parquet
df.write.mode("overwrite").parquet(
    "s3://foodhub-datalake-team3/processed/orders/"
)