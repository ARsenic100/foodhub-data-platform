# python3 << 'EOF'
import boto3, json, random, uuid
from datetime import datetime

# CHANGE YOUR_NAME below
kinesis  = boto3.client('kinesis', region_name='us-east-1')
STREAM   = 'foodhub-orders-stream-YOUR_NAME'
CITIES   = ["Bangalore","Hyderabad","Chennai","Mumbai","Pune"]
STATUSES = ["DELIVERED","DELIVERED","DELIVERED","PENDING","CANCELLED"]
PAYMENTS = ["UPI","Debit Card","Credit Card","Cash on Delivery","Wallet"]

sent = 0
for i in range(50):
    try:
        order = {
            "order_id"        : f"FH{random.randint(1000,9999)}",
            "customer_id"     : f"C{random.randint(1,100):03d}",
            "order_date"      : datetime.utcnow().strftime("%Y-%m-%d"),
            "order_time"      : datetime.utcnow().strftime("%H:%M:%S"),
            "status"          : random.choice(STATUSES),
            "total_amount"    : round(random.uniform(150, 2000), 2),
            "delivery_minutes": random.randint(20, 60),
            "payment_method"  : random.choice(PAYMENTS),
            "city"            : random.choice(CITIES),
            "event_time"      : datetime.utcnow().isoformat()
        }
        kinesis.put_record(
            StreamName=STREAM, Data=json.dumps(order),
            PartitionKey=order["order_id"]
        )
        sent += 1
        print(f"Sent: {order['order_id']} | {order['city']}")
    except Exception as e:
        print(f"Failed: {e}")
print(f"\nTotal sent: {sent}")