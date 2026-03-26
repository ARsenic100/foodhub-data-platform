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