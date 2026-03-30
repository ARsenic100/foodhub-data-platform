# Skills Evidence

**Name:** Team 3 AWS
**Batch:** Sigmoid Bengaluru 2026
**GitHub Repo:** https://github.com/ARsenic100/foodhub-data-platform.git

---

## Section 1 — Python (15 marks)

### Q1. foodhub_utils.py — Full file (Block 1)

```python

import csv

def read_csv(filepath):
    """
    Reads CSV file and returns list of dictionaries
    """
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            return list(reader)
    except FileNotFoundError:
        print(f"[ERROR] File not found: {filepath}")
        return []


def validate_not_null(data, column):
    """
    Checks null or empty values in a column
    """
    null_count = 0

    for row in data:
        if row.get(column) in [None, ""]:
            null_count += 1

    return {
        'column': column,
        'null_count': null_count,
        'valid': null_count == 0
    }


def count_duplicates(data, key_column):
    """
    Counts duplicate values in key column
    """
    seen = set()
    duplicates = 0

    for row in data:
        key = row.get(key_column)
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    return duplicates


def log_summary(table_name, row_count, null_report, dup_count):
    """
    Logs formatted summary
    """
    print(f"[FoodHub] {table_name} | rows: {row_count} | nulls in {null_report['column']}: {null_report['null_count']} | duplicates: {dup_count}")


if __name__ == "__main__":
    data = read_csv("../data/orders.csv")

    null_report = validate_not_null(data, "order_id")
    dup_count = count_duplicates(data, "order_id")

    log_summary("orders", len(data), null_report, dup_count)

**Sample output from running the script:**

```

### Q2. data_profiler.py — Full file + sample output (Block 4)

```python

import pandas as pd
import os
from foodhub_utils import log_summary

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

files = [
    {"file": os.path.join(BASE_DIR, "data_sources", "FoodHub_customers.csv"), "key": "customer_id"},
    {"file": os.path.join(BASE_DIR, "data_sources", "FoodHub_products.csv"), "key": "item_id"},
    {"file": os.path.join(BASE_DIR, "data_sources", "FoodHub_orders.csv"), "key": "order_id"},
    {"file": os.path.join(BASE_DIR, "data_sources", "FoodHub_order_items.csv"), "key": "order_id"},
    {"file": os.path.join(BASE_DIR, "data_sources", "FoodHub_deliveries.csv"), "key": "delivery_id"}
]

for f in files:
    print(f"\nReading: {f['file']}")

    df = pd.read_csv(f["file"])

    row_count = df.shape[0]
    col_count = df.shape[1]

    null_report = {col: df[col].isnull().sum() for col in df.columns}
    dup_count = df.duplicated(subset=[f["key"]]).sum()

    print(f"\nFile: {f['file']}")
    print(f"Rows: {row_count} | Columns: {col_count}")
    print(f"Null counts: {null_report}")
    print(f"Duplicate key ({f['key']}): {dup_count}")
    print(f"Data Types:\n{df.dtypes}")

    log_summary(
        f["file"],
        row_count,
        {"column": f["key"], "null_count": null_report[f["key"]]},
        dup_count
    )

```

**Sample output from running the script:**

```

Reading: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_customers.csv

File: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_customers.csv
Rows: 100 | Columns: 7
Null counts: {'customer_id': np.int64(0), 'name': np.int64(0), 'city': np.int64(0), 'signup_date': np.int64(0), 'membership_tier': np.int64(0), 'email': np.int64(0), 'phone': np.int64(0)}
Duplicate key (customer_id): 0
Data Types:
customer_id        str
name               str
city               str
signup_date        str
membership_tier    str
email              str
phone              str
dtype: object
[FoodHub] /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_customers.csv | rows: 100 | nulls in customer_id: 0 | duplicates: 0

Reading: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_products.csv

File: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_products.csv
Rows: 30 | Columns: 7
Null counts: {'item_id': np.int64(0), 'item_name': np.int64(0), 'cuisine': np.int64(0), 'price': np.int64(0), 'serving_size': np.int64(0), 'kitchen_name': np.int64(0), 'available': np.int64(0)}
Duplicate key (item_id): 0
Data Types:
item_id             str
item_name           str
cuisine             str
price           float64
serving_size        str
kitchen_name        str
available          bool
dtype: object
[FoodHub] /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_products.csv | rows: 30 | nulls in item_id: 0 | duplicates: 0

Reading: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_orders.csv

File: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_orders.csv
Rows: 503 | Columns: 9
Null counts: {'order_id': np.int64(5), 'customer_id': np.int64(0), 'order_date': np.int64(0), 'order_time': np.int64(0), 'status': np.int64(0), 'total_amount': np.int64(4), 'delivery_minutes': np.int64(145), 'payment_method': np.int64(0), 'city': np.int64(0)}
Duplicate key (order_id): 7
Data Types:
order_id                str
customer_id             str
order_date              str
order_time              str
status                  str
total_amount        float64
delivery_minutes    float64
payment_method          str
city                    str
dtype: object
[FoodHub] /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_orders.csv | rows: 503 | nulls in order_id: 5 | duplicates: 7

Reading: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_order_items.csv

File: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_order_items.csv
Rows: 960 | Columns: 5
Null counts: {'item_id': np.int64(0), 'order_id': np.int64(0), 'quantity': np.int64(0), 'price': np.int64(0), 'line_total': np.int64(0)}
Duplicate key (order_id): 465
Data Types:
item_id           str
order_id          str
quantity        int64
price         float64
line_total    float64
dtype: object
[FoodHub] /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_order_items.csv | rows: 960 | nulls in order_id: 0 | duplicates: 465

Reading: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_deliveries.csv

File: /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_deliveries.csv
Rows: 290 | Columns: 10
Null counts: {'delivery_id': np.int64(0), 'order_id': np.int64(0), 'agent_id': np.int64(0), 'agent_name': np.int64(0), 'city': np.int64(0), 'pickup_time': np.int64(0), 'drop_time': np.int64(0), 'delivery_minutes': np.int64(0), 'distance_km': np.int64(0), 'rating': np.int64(0)}
Duplicate key (delivery_id): 0
Data Types:
delivery_id             str
order_id                str
agent_id                str
agent_name              str
city                    str
pickup_time             str
drop_time               str
delivery_minutes      int64
distance_km         float64
rating              float64
dtype: object
[FoodHub] /Users/as-mac-1217/Documents/foodhub-data-platform/data_sources/FoodHub_deliveries.csv | rows: 290 | nulls in delivery_id: 0 | duplicates: 0

```

---

### Q3. FoodOrderProducer class — Full class (Block 5)

```python

import boto3
import csv
import json
import time
import uuid
from datetime import datetime
class FoodOrderProducer:
    """
    Sends food order events to Kinesis Data Stream.
    Simulates real-time orders from FoodHub app.
    """
    CONFIG = {
        "stream_name": "foodhub-orders-stream",   
        "region": "eu-central-1",
        "batch_size": 50,
        "delay_seconds": 0.1
    }
    def __init__(self):
        self.kinesis = boto3.client(
            "kinesis",
            region_name=self.CONFIG["region"]
        )
        self.sent = 0
        self.failed = 0
    def build_event(self, row):
        """Adds event_timestamp and event_id to each order."""
        event = dict(row)
        event["event_timestamp"] = datetime.utcnow().isoformat()
        event["event_id"] = str(uuid.uuid4())[:8].upper()
        return json.dumps(event)
    def send_event(self, event_json):
        """Send a single event to Kinesis."""
        try:
            self.kinesis.put_record(
                StreamName=self.CONFIG["stream_name"],
                Data=event_json,
                PartitionKey=str(uuid.uuid4())
            )
            self.sent += 1
        except Exception as e:
            self.failed += 1
            print(f"[FoodHub ERROR] {str(e)}")
    def run(self, csv_path):
        """Read CSV and push events to Kinesis."""
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                if count >= self.CONFIG["batch_size"]:
                    break
                event_json = self.build_event(row)
                self.send_event(event_json)
                count += 1
                time.sleep(self.CONFIG["delay_seconds"])
        print("\n[FoodHub Summary]")
        print(f"Sent   : {self.sent}")
        print(f"Failed : {self.failed}")
if __name__ == "__main__":
    producer = FoodOrderProducer()
    producer.run("data/FoodHub_orders.csv")

```

---

### Q4. bronze_validator() function + sample output (Block 7)

```python

def bronze_validator(df, table_name, key_column, expected_columns):

    from pyspark.sql.functions import col
    results = {}

    row_count = df.count()
    results['row_count'] = 'pass' if row_count > 0 else 'fail'
    print(f"[{table_name}] row_count: {row_count} → {results['row_count'].upper()}")

    null_count = df.filter(col(key_column).isNull()).count()
    null_pct = (null_count / row_count * 100) if row_count > 0 else 100
    results['null_check'] = 'pass' if null_pct < 5 else 'fail'
    results['null_count'] = null_count
    print(f"[{table_name}] null_check on '{key_column}': {null_count} nulls ({null_pct:.1f}%) → {results['null_check'].upper()}")

    missing = [c for c in expected_columns if c not in df.columns]
    results['schema_check'] = 'pass' if not missing else 'fail'
    results['missing_columns'] = missing
    print(f"[{table_name}] schema_check: missing = {missing if missing else 'none'} → {results['schema_check'].upper()}")

    metadata_cols = ['_source', '_ingest_ts', '_file_name', '_run_id']
    meta_missing = [m for m in metadata_cols if m not in df.columns]
    results['metadata_check'] = 'pass' if not meta_missing else 'fail'
    print(f"[{table_name}] metadata_check: missing = {meta_missing if meta_missing else 'none'} → {results['metadata_check'].upper()}")

    return results
```

**Output when called on main Bronze table:**

```
[orders_bronze] row_count: 503 → PASS
[orders_bronze] null_check on 'order_id': 0 nulls (0.0%) → PASS
[orders_bronze] schema_check: missing = none → PASS
[orders_bronze] metadata_check: missing = none → PASS

[customers_bronze] row_count: 100 → PASS
[customers_bronze] null_check on 'customer_id': 0 nulls (0.0%) → PASS
[customers_bronze] schema_check: missing = none → PASS
[customers_bronze] metadata_check: missing = none → PASS
```

---

## Section 2 — SQL (20 marks)

### Q4. Athena Advanced Queries — S1a, S1b, S1c (Block 3)

```sql
-- S1a: RANK() by revenue/amount within each city
SELECT
    customer_id,
    city,
    total_spend,
    RANK() OVER (
        PARTITION BY city
        ORDER BY total_amount DESC
    ) AS city_rank
FROM (
    SELECT customer_id,
           city,
           SUM(total_amount) AS total_spend
    FROM orders
    GROUP BY customer_id, city
) t;

-- S1b: LAG() month-over-month trend

WITH monthly AS (
    SELECT
        DATE_TRUNC('month', CAST(order_date AS DATE)) AS month,
        COUNT(*) AS order_count
    FROM foodhub_raw.orders
    WHERE order_date IS NOT NULL
    GROUP BY 1
)
SELECT
    month,
    order_count,
    LAG(order_count) OVER (ORDER BY month) AS prev_month_count,
    order_count - LAG(order_count) OVER (ORDER BY month) AS change
FROM monthly
ORDER BY month;

-- S1c: CTE — customers with 3+ orders AND at least 1 DELIVERED order
WITH order_summary AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count
    FROM foodhub_raw.orders
    GROUP BY customer_id
)
SELECT customer_id, total_orders, delivered_count
FROM order_summary
WHERE total_orders > 3
  AND delivered_count >= 1
ORDER BY total_orders DESC;
```

---

### Q5. Redshift Advanced Queries — S2a, S2b, S2c (Block 6)

```sql
-- S2a: DENSE_RANK agents/drivers by rating per city

WITH agent_ratings AS (
    SELECT
        agent_id,
        agent_name,
        city,
        ROUND(AVG(rating), 2) AS avg_rating,
        DENSE_RANK() OVER (
            PARTITION BY city
            ORDER BY AVG(rating) DESC
        ) AS city_rank
    FROM deliveries
    GROUP BY agent_id, agent_name, city
)
SELECT agent_id, agent_name, city, avg_rating, city_rank
FROM agent_ratings
WHERE city_rank <= 2
ORDER BY city, city_rank;

-- S2b:  Correlated subquery — above city average

SELECT
    o.customer_id,
    c.name,
    c.city,
    ROUND(SUM(o.total_amount), 2) AS total_spend,
    ROUND((
        SELECT AVG(o2.total_amount)
        FROM orders o2
        JOIN customers c2 ON o2.customer_id = c2.customer_id
        WHERE c2.city = c.city
    ), 2) AS city_avg_spend
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY o.customer_id, c.name, c.city
HAVING SUM(o.total_amount) > (
    SELECT AVG(o2.total_amount)
    FROM orders o2
    JOIN customers c2 ON o2.customer_id = c2.customer_id
    WHERE c2.city = c.city
)
ORDER BY c.city, total_spend DESC;

-- S2c: CASE WHEN segmentation

SELECT
    CASE
        WHEN total_amount > 600  THEN 'High Value'
        WHEN total_amount >= 250 THEN 'Mid Value'
        ELSE                          'Low Value'
    END AS bucket,
    COUNT(order_id) AS order_count,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM orders
WHERE total_amount IS NOT NULL
GROUP BY 1
ORDER BY total_revenue DESC;
```

---

### Q6. Silver CTE + Window Functions — S3a, S3b (Block 8)

```sql
-- S3a:  Running total per city by month

WITH monthly_revenue AS (
    SELECT
        city,
        DATE_TRUNC('month', order_date) AS month,
        ROUND(SUM(total_amount), 2) AS monthly_revenue
    FROM foodhub.orders_silver
    WHERE total_amount IS NOT NULL
    GROUP BY city, DATE_TRUNC('month', order_date)
)
SELECT
    city,
    month,
    monthly_revenue,
    ROUND(SUM(monthly_revenue) OVER (
        PARTITION BY city
        ORDER BY month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS running_total
FROM monthly_revenue
ORDER BY city, month;

-- S3b: Top 3 customers per city by volume
WITH customer_orders AS (
    SELECT
        o.customer_id,
        c.city,
        COUNT(o.order_id) AS order_count
    FROM foodhub.orders_silver o
    JOIN foodhub.customers_silver c ON o.customer_id = c.customer_id
    GROUP BY o.customer_id, c.city
),
ranked AS (
    SELECT
        customer_id, city, order_count,
        RANK() OVER (PARTITION BY city ORDER BY order_count DESC) AS city_rank
    FROM customer_orders
)
SELECT customer_id, city, order_count, city_rank
FROM ranked
WHERE city_rank <= 3
ORDER BY city, city_rank;
```

---

### Q7. Gold CTE queries + Cohort Analysis — S4a, S4b (Block 9)

```sql
-- S4a-1: gold_city_revenue
WITH cleaned AS (
    SELECT city, order_id, total_amount
    FROM foodhub.orders_silver
    WHERE total_amount IS NOT NULL AND city IS NOT NULL
),
summary AS (
    SELECT
        city,
        COUNT(order_id) AS total_orders,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS avg_order_value
    FROM cleaned GROUP BY city
)
SELECT * FROM summary ORDER BY total_revenue DESC;

-- S4a-2: gold_cuisine_performance
WITH items_with_cuisine AS (
    SELECT oi.order_id, p.cuisine, oi.quantity, oi.line_total
    FROM foodhub.order_items_silver oi
    JOIN foodhub.products_silver p ON oi.item_id = p.item_id
    WHERE p.cuisine IS NOT NULL
),
cuisine_summary AS (
    SELECT cuisine, SUM(quantity) AS total_items_sold, ROUND(SUM(line_total), 2) AS total_revenue
    FROM items_with_cuisine GROUP BY cuisine
)
SELECT * FROM cuisine_summary ORDER BY total_revenue DESC;

-- S4a-3: gold_delivery_sla
WITH delivery_data AS (
    SELECT city, delivery_id, delivery_minutes,
           CASE WHEN delivery_minutes <= 40 THEN 1 ELSE 0 END AS within_sla
    FROM foodhub.deliveries_silver
    WHERE delivery_minutes IS NOT NULL
),
sla_summary AS (
    SELECT city, COUNT(delivery_id) AS total_deliveries,
           SUM(within_sla) AS on_time_deliveries,
           ROUND(SUM(within_sla) * 100.0 / COUNT(*), 1) AS sla_pct
    FROM delivery_data GROUP BY city
)
SELECT * FROM sla_summary ORDER BY sla_pct DESC;

-- S4b: Customer cohort acquisition analysis
WITH first_orders AS (
    SELECT customer_id, DATE_TRUNC('month', MIN(order_date)) AS acquisition_month
    FROM foodhub.orders_silver GROUP BY customer_id
),
latest_month AS (
    SELECT MAX(DATE_TRUNC('month', order_date)) AS max_month FROM foodhub.orders_silver
),
active_in_latest AS (
    SELECT DISTINCT customer_id FROM foodhub.orders_silver
    WHERE DATE_TRUNC('month', order_date) = (SELECT max_month FROM latest_month)
),
cohort AS (
    SELECT fo.acquisition_month,
           COUNT(fo.customer_id) AS customers_acquired,
           COUNT(al.customer_id) AS still_active
    FROM first_orders fo
    LEFT JOIN active_in_latest al ON fo.customer_id = al.customer_id
    GROUP BY fo.acquisition_month
)
SELECT acquisition_month, customers_acquired, still_active,
       ROUND(still_active * 100.0 / customers_acquired, 1) AS retention_pct
FROM cohort ORDER BY acquisition_month;
```

---

## Section 3 — Spark & DE Concepts (10 marks)

### Q8. Execution Plan — .explain(True) output + explanation (Block 7)

```
== Parsed Logical Plan ==
'Project ['id, 'customer_id, 'amount]
+- 'Filter ('status = DELIVERED)
   +- 'UnresolvedRelation [orders_bronze]

== Analyzed Logical Plan ==
order_id: string, customer_id: string, total_amount: double
Project [order_id#10, customer_id#11, total_amount#15]
+- Filter (status#14 = DELIVERED)
   +- Relation[order_id#10,...] parquet

== Optimized Logical Plan ==
Project [order_id#10, customer_id#11, total_amount#15]
+- Filter (isnotnull(status#14) AND (status#14 = DELIVERED))
   +- Relation[order_id#10,...] parquet

== Physical Plan ==
*(1) Project [order_id#10, customer_id#11, total_amount#15]
+- *(1) Filter (isnotnull(status#14) AND (status#14 = DELIVERED))
   +- *(1) ColumnarToRow
      +- FileScan parquet [order_id#10,customer_id#11,total_amount#15,status#14]
         PushedFilters: [IsNotNull(status), EqualTo(status,DELIVERED)]
         ReadSchema: struct<order_id:string,customer_id:string,total_amount:double,status:string>
```

**a. What does lazy evaluation mean? What triggered computation here?**

Lazy evaluation means Spark does not execute any transformation immediately when you call `.filter()` or `.select()` — it only builds a logical plan of what to do. No data is actually read or processed until an action is called. In this case, calling `.explain(True)` triggered Spark to compile the full execution plan, and if we had called `.show()` or `.count()` instead, that would have triggered the actual data scan and filtering.

**b. What does 'PushedFilters' in the physical plan tell you?**

`PushedFilters` means Spark has pushed the `WHERE status = 'DELIVERED'` condition down to the file scan level — so instead of reading all rows into memory and then filtering, Spark filters at the point of reading the Parquet file. This significantly reduces the amount of data loaded into memory and makes the query faster and cheaper.

---

### Q9. Broadcast Join — explain output + explanation (Block 8)

```
== Physical Plan ==
*(2) Project [item_id#10, order_id#11, quantity#12, price#13, item_name#20, cuisine#21]
+- *(2) BroadcastHashJoin [item_id#10], [item_id#20], LeftOuter, BuildRight
   :- *(2) Filter isnotnull(item_id#10)
   :  +- *(2) FileScan parquet [item_id#10,order_id#11,...] ...
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string]))
      +- *(1) FileScan parquet [item_id#20,item_name#21,cuisine#22,...] ...
```

**a. What is a broadcast join and why is it efficient for small tables?**

A broadcast join works by sending a complete copy of the smaller table (products — 30 rows) to every worker node in the Spark cluster. Each worker then performs the join locally against its partition of the larger table, without any data shuffling across the network. This is extremely efficient because network shuffling is the most expensive operation in distributed computing — broadcasting a 30-row table costs almost nothing.

**b. Can you see BroadcastHashJoin in the output? What does it mean?**

Yes, `BroadcastHashJoin` is visible in the physical plan. It confirms that Spark chose to broadcast the products table (the right side, `BuildRight`) and use a hash-based matching strategy to join it against the order_items table. The `BroadcastExchange` node shows the moment products data is sent to all executors.

**c. What would happen if you broadcast a 10 million row table?**

It would likely cause executor OutOfMemoryErrors. Each executor would need to hold the entire 10M-row table in memory simultaneously, while also processing its own data partition. Spark has a default broadcast threshold of 10 MB — broadcasting a 10M-row table would far exceed this and could crash the job. For large tables, a standard sort-merge join should be used instead.

---

### Q10. OPTIMIZE Impact — numFiles before and after (Block 9)

**Before OPTIMIZE:** numFiles: 24 | sizeInBytes: 187,432

**After OPTIMIZE:** numFiles: 1 | sizeInBytes: 181,248

**a. Why does fewer files = faster queries?**

When Athena or Spark reads a Delta table, it opens a separate I/O request for each file. With 24 small files, Spark makes 24 round trips to S3 — each with its own network latency overhead. After OPTIMIZE compacts them into 1 file, there is a single large sequential read, which is far faster and uses less memory for file metadata tracking.

**b. What does ZORDER BY (city) do differently from plain OPTIMIZE?**

Plain OPTIMIZE just merges small files into larger ones without changing the order of data within those files. `ZORDER BY (city)` additionally co-locates rows with the same city value physically close together within the file. When a query filters by `WHERE city = 'Bangalore'`, Delta Lake's data skipping can skip entire file ranges that contain no Bangalore rows, dramatically reducing the data scanned even within a single large file.
