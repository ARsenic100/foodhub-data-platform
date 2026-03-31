# Databricks notebook source
DB = "foodhub_team3"

spark.sql(f"USE {DB}")

# COMMAND ----------

orders_df = spark.read.table(f"{DB}.orders_bronze")

# COMMAND ----------

from pyspark.sql.functions import col, upper, current_date

orders_clean = orders_df \
    .dropna(subset=["order_id"]) \
    .dropDuplicates(["event_id"]) \
    .withColumn("total_amount", col("total_amount").cast("double")) \
    .withColumn("delivery_minutes", col("delivery_minutes").cast("int")) \
    .withColumn("status", upper(col("status"))) \
    .withColumn("order_date", col("order_date").cast("date")) \
    .withColumn("processing_date", current_date())

# COMMAND ----------

print("Before Cleaning:", orders_df.count())
print("After Cleaning:", orders_clean.count())

# COMMAND ----------

orders_clean.write.format("delta").mode("overwrite").saveAsTable(f"{DB}.orders_silver")

# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, f"{DB}.orders_silver")

silver_table.alias("t").merge(
    orders_clean.alias("s"),
    "t.order_id = s.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

customers_df = spark.read.table(f"{DB}.customers_bronze")

# COMMAND ----------

customers_clean = customers_df \
    .dropna(subset=["customer_id"]) \
    .dropDuplicates(["customer_id"]) \
    .withColumn("processing_date", current_date())

# COMMAND ----------

customers_clean.write.format("delta").mode("overwrite").saveAsTable(f"{DB}.customers_silver")

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, f"{DB}.customers_silver")

silver_table.alias("t").merge(
    customers_clean.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# COMMAND ----------

products_df = spark.read.table(f"{DB}.products_bronze")

products_clean = products_df \
    .dropDuplicates(["item_id"]) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("processing_date", current_date())

products_clean.write.format("delta").mode("overwrite").saveAsTable(f"{DB}.products_silver")

# COMMAND ----------

items_df = spark.read.table(f"{DB}.order_items_bronze")

items_clean = items_df \
    .dropDuplicates(["item_id"]) \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("quantity", col("quantity").cast("int")) \
    .withColumn("processing_date", current_date())

items_clean.write.format("delta").mode("overwrite").saveAsTable(f"{DB}.order_items_silver")

# COMMAND ----------

deliveries_df = spark.read.table(f"{DB}.deliveries_bronze")

deliveries_clean = deliveries_df \
    .dropDuplicates(["delivery_id"]) \
    .withColumn("delivery_minutes", col("delivery_minutes").cast("int")) \
    .withColumn("processing_date", current_date())

deliveries_clean.write.format("delta").mode("overwrite").saveAsTable(f"{DB}.deliveries_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE foodhub_team3.orders_silver
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly_revenue AS (
# MAGIC     SELECT 
# MAGIC         city,
# MAGIC         date_trunc('month', order_date) AS month,
# MAGIC         SUM(total_amount) AS monthly_revenue
# MAGIC     FROM foodhub_team3.orders_silver
# MAGIC     GROUP BY city, month
# MAGIC )
# MAGIC
# MAGIC SELECT *,
# MAGIC        SUM(monthly_revenue) OVER (
# MAGIC            PARTITION BY city
# MAGIC            ORDER BY month
# MAGIC            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC        ) AS running_total
# MAGIC FROM monthly_revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH customer_orders AS (
# MAGIC
# MAGIC
# MAGIC SELECT 
# MAGIC     customer_id,
# MAGIC     city,
# MAGIC     COUNT(*) AS order_count
# MAGIC     FROM foodhub_team3.orders_silver
# MAGIC     GROUP BY customer_id, city
# MAGIC )
# MAGIC
# MAGIC SELECT *,
# MAGIC     RANK() OVER (
# MAGIC         PARTITION BY city
# MAGIC         ORDER BY order_count DESC
# MAGIC     ) AS city_rank
# MAGIC FROM customer_orders
# MAGIC QUALIFY city_rank <= 3;

# COMMAND ----------

from pyspark.sql.functions import broadcast

products_df = spark.read.table("foodhub_team3.products_silver")

result = items_clean.join(
    broadcast(products_df),
    "item_id",
    "left"
)

result.explain(True)

# COMMAND ----------



# COMMAND ----------

