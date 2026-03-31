# Databricks notebook source
YOUR_NAME = "team3"   # same as your S3 bucket

CATALOG = "main"
DB = f"foodhub_{YOUR_NAME}"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB}")
spark.sql(f"USE {DB}")

print("Database Ready ✅")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# COMMAND ----------

orders_df = spark.read.json(
    "s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/streaming/2026/03/27/17/",
)
orders_df.display()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_timestamp, lit
import uuid

run_id = str(uuid.uuid4())

orders_df = orders_df \
    .withColumn("ingest_date", to_date(col("ingest_date"))) \
    .withColumn("_source", lit("kinesis_stream")) \
    .withColumn("_ingest_ts", current_timestamp()) \
    .withColumn("_file_name", col("_metadata.file_path")) \
    .withColumn("_run_id", lit(run_id))

# COMMAND ----------

orders_df.write.format("delta") \
    .mode("append") \
    .partitionBy("ingest_date") \
    .save("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/orders")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS foodhub_team3.orders_bronze
USING DELTA
LOCATION 's3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/orders'
""")

# COMMAND ----------

from pyspark.errors import AnalysisException

try:
    spark.sql("""
    ALTER TABLE foodhub_team3.orders_bronze
    ADD CONSTRAINT valid_order_id CHECK (order_id IS NOT NULL)
    """)
except AnalysisException as e:
    if "DELTA_CONSTRAINT_ALREADY_EXISTS" not in str(e):
        raise

try:
    spark.sql("""
    ALTER TABLE foodhub_team3.orders_bronze
    ADD CONSTRAINT valid_amount CHECK (total_amount >= 0)
    """)
except AnalysisException as e:
    if "DELTA_CONSTRAINT_ALREADY_EXISTS" not in str(e):
        raise

# COMMAND ----------

customers_df = spark.read.format("csv") \
    .option("header", True) \
    .load("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/raw/customers/")

# COMMAND ----------

customers_df.display()

# COMMAND ----------

customers_df = customers_df \
    .withColumn("ingest_date", current_timestamp()) \
    .withColumn("_source", lit("batch_file")) \
    .withColumn("_ingest_ts", current_timestamp()) \
    .withColumn("_file_name", col("_metadata.file_path")) \
    .withColumn("_run_id", lit(run_id))

# COMMAND ----------

customers_df.write.format("delta") \
    .mode("append") \
    .partitionBy("ingest_date") \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .save("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/customers")

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS foodhub_team3.customers_bronze
USING DELTA
LOCATION 's3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/customers'
""")

# COMMAND ----------

def bronze_validator(df, table_name, key_column, expected_columns):
    results = {}

    total_count = df.count()
    results["row_count"] = "pass" if total_count > 0 else "fail"

    null_count = df.filter(col(key_column).isNull()).count()
    null_percent = (null_count / total_count) * 100 if total_count > 0 else 0

    results["null_check"] = {
        "status": "pass" if null_percent < 5 else "fail",
        "null_count": null_count
    }

    missing_cols = [c for c in expected_columns if c not in df.columns]

    results["schema_check"] = {
        "status": "pass" if len(missing_cols) == 0 else "fail",
        "missing_columns": missing_cols
    }

    metadata_cols = ["_source", "_ingest_ts", "_file_name", "_run_id"]
    metadata_missing = [c for c in metadata_cols if c not in df.columns]

    results["metadata_check"] = "pass" if len(metadata_missing) == 0 else "fail"

    print(f"\nValidation Report for {table_name}")
    print(results)

    return results

# COMMAND ----------

orders_df_check = spark.read.format("delta").load(
"s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/orders"
)

bronze_validator(
    orders_df_check,
    "orders_bronze",
    "order_id",
    orders_df_check.columns
)

# COMMAND ----------

spark.sql("SHOW TABLES IN foodhub_team3").show()

# COMMAND ----------

products_df = spark.read.format("csv") \
    .option("header", True) \
    .load("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/raw/products/")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col
import uuid

run_id = str(uuid.uuid4())

def add_metadata(df):
    return df \
        .withColumn("ingest_date", current_timestamp()) \
        .withColumn("_source", lit("batch_file")) \
        .withColumn("_ingest_ts", current_timestamp()) \
        .withColumn("_file_name", col("_metadata.file_path")) \
        .withColumn("_run_id", lit(run_id))

# COMMAND ----------

products_df = add_metadata(products_df)


# COMMAND ----------

products_df.write.format("delta") \
    .mode("append") \
    .partitionBy("ingest_date") \
    .save("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/products")

# COMMAND ----------

order_items_df = spark.read.format("csv") \
    .option("header", True) \
    .load("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/raw/order_items/")

# COMMAND ----------

order_items_df = add_metadata(order_items_df)

# COMMAND ----------

order_items_df.write.format("delta") \
    .mode("append") \
    .partitionBy("ingest_date") \
    .save("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/order_items")

# COMMAND ----------

deliveries_df = spark.read.format("csv") \
    .option("header", True) \
    .load("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/raw/deliveries/")

# COMMAND ----------

deliveries_df = add_metadata(deliveries_df)

# COMMAND ----------

deliveries_df.write.format("delta") \
    .mode("append") \
    .partitionBy("ingest_date") \
    .save("s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/deliveries")

# COMMAND ----------

base_path = "s3://s3-de-q1-26/DE-Training/foodhub_datalake_team3/bronze/"

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS foodhub_team3.orders_bronze
USING DELTA LOCATION '{base_path}orders'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS foodhub_team3.customers_bronze
USING DELTA LOCATION '{base_path}customers'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS foodhub_team3.products_bronze
USING DELTA LOCATION '{base_path}products'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS foodhub_team3.order_items_bronze
USING DELTA LOCATION '{base_path}order_items'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS foodhub_team3.deliveries_bronze
USING DELTA LOCATION '{base_path}deliveries'
""")

# COMMAND ----------

spark.sql("SHOW TABLES IN foodhub_team3").show()

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM foodhub_team3.orders_bronze").show()
spark.sql("SELECT COUNT(*) FROM foodhub_team3.customers_bronze").show()
spark.sql("SELECT COUNT(*) FROM foodhub_team3.products_bronze").show()
spark.sql("SELECT COUNT(*) FROM foodhub_team3.order_items_bronze").show()
spark.sql("SELECT COUNT(*) FROM foodhub_team3.deliveries_bronze").show()

# COMMAND ----------

