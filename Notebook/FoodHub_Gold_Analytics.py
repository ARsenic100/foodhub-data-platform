# Databricks notebook source
spark.sql("USE foodhub_team3")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Gold Table: City Revenue
# MAGIC CREATE OR REPLACE TABLE foodhub_team3.gold_city_revenue
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH cleaned AS (
# MAGIC     SELECT 
# MAGIC         city,
# MAGIC         order_id,
# MAGIC         total_amount
# MAGIC     FROM foodhub_team3.orders_silver
# MAGIC     WHERE total_amount IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC summary AS (
# MAGIC     SELECT 
# MAGIC         city,
# MAGIC         COUNT(order_id) AS total_orders,
# MAGIC         SUM(total_amount) AS total_revenue,
# MAGIC         AVG(total_amount) AS avg_order_value
# MAGIC     FROM cleaned
# MAGIC     GROUP BY city
# MAGIC )
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM summary
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE foodhub_team3.gold_city_revenue
# MAGIC  ZORDER BY (city);

# COMMAND ----------

display(spark.table("foodhub_team3.gold_city_revenue"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE foodhub_team3.gold_cuisine_performance
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH joined AS (
# MAGIC     SELECT 
# MAGIC         p.cuisine,
# MAGIC         oi.quantity,
# MAGIC         oi.price
# MAGIC     FROM foodhub_team3.order_items_silver oi
# MAGIC     JOIN foodhub_team3.products_silver p
# MAGIC         ON oi.item_id = p.item_id
# MAGIC ),
# MAGIC
# MAGIC summary AS (
# MAGIC     SELECT 
# MAGIC         cuisine,
# MAGIC         SUM(quantity) AS total_items_sold,
# MAGIC         SUM(quantity * price) AS total_revenue
# MAGIC     FROM joined
# MAGIC     GROUP BY cuisine
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM summary
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE foodhub_team3.gold_cuisine_performance
# MAGIC ZORDER BY (cuisine);

# COMMAND ----------

display(spark.table("foodhub_team3.gold_cuisine_performance"))

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC CREATE OR REPLACE TABLE foodhub_team3.gold_delivery_sla
# MAGIC USING DELTA AS
# MAGIC
# MAGIC WITH cleaned AS (
# MAGIC     SELECT 
# MAGIC         city,
# MAGIC         delivery_minutes
# MAGIC     FROM foodhub_team3.deliveries_silver
# MAGIC     WHERE delivery_minutes IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC sla_calc AS (
# MAGIC     SELECT 
# MAGIC         city,
# MAGIC         COUNT(*) AS total_deliveries,
# MAGIC         SUM(CASE WHEN delivery_minutes <= 40 THEN 1 ELSE 0 END) AS within_40
# MAGIC     FROM cleaned
# MAGIC     GROUP BY city
# MAGIC ),
# MAGIC
# MAGIC final AS (
# MAGIC     SELECT 
# MAGIC         city,
# MAGIC         (within_40 * 100.0 / total_deliveries) AS sla_percentage
# MAGIC     FROM sla_calc
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM final
# MAGIC ORDER BY sla_percentage DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE foodhub_team3.gold_delivery_sla
# MAGIC ZORDER BY (city);
# MAGIC

# COMMAND ----------

display(spark.table("foodhub_team3.gold_delivery_sla"))

# COMMAND ----------

display(spark.sql("""
                  WITH first_txn AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', order_date) AS order_month,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY order_date
        ) AS rn
    FROM foodhub_team3.orders_silver
),

acquisition AS (
    SELECT 
        customer_id,
        order_month AS acquisition_month
    FROM first_txn
    WHERE rn = 1
),

recent_month AS (
    SELECT 
        MAX(DATE_TRUNC('month', order_date)) AS latest_month
    FROM foodhub_team3.orders_silver
),

active_customers AS (
    SELECT DISTINCT customer_id
    FROM foodhub_team3.orders_silver
    WHERE DATE_TRUNC('month', order_date) = (SELECT latest_month FROM recent_month)
),

final AS (
    SELECT 
        a.acquisition_month,
        COUNT(DISTINCT a.customer_id) AS customers_acquired,
        COUNT(DISTINCT CASE 
            WHEN ac.customer_id IS NOT NULL THEN a.customer_id 
        END) AS still_active
    FROM acquisition a
    LEFT JOIN active_customers ac
        ON a.customer_id = ac.customer_id
    GROUP BY a.acquisition_month
)

SELECT *
FROM final
ORDER BY acquisition_month;
"""))

# COMMAND ----------

spark.sql("DESCRIBE DETAIL foodhub_team3.gold_city_revenue").show(truncate=False)

# COMMAND ----------

spark.sql("SELECT current_database()").show()

# COMMAND ----------

spark.sql("USE foodhub_team3")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

