import boto3, json, random, uuid
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='eu-central-1')

STREAM = 'foodhub-orders-stream'

STATUSES = ["DELIVERED", "SHIPPED", "PENDING", "CANCELLED"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Wallet"]
CITIES = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Pune", "Hyderabad"]


def lambda_handler(event, context):
    sent = 0
    failed = 0

    for i in range(10):   # send 10 events per trigger
        try:
            order = {
                "order_id": f"FH{random.randint(1,5000):04d}",
                "customer_id": f"C{random.randint(1,100):03d}",
                "order_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "order_time": datetime.utcnow().strftime("%H:%M:%S"),
                "status": random.choice(STATUSES),
                "total_amount": round(random.uniform(50, 1500), 2),
                "delivery_minutes": random.choice([None, random.randint(10, 45)]),
                "payment_method": random.choice(PAYMENT_METHODS),
                "city": random.choice(CITIES),

                # same style as RideWave
                "event_time": datetime.utcnow().isoformat(),
                "ingest_date": datetime.utcnow().strftime("%Y-%m-%d"),
                "event_id": str(uuid.uuid4())
            }

            kinesis.put_record(
                StreamName=STREAM,
                Data=json.dumps(order),
                PartitionKey=order["order_id"]
            )

            sent += 1
            print(f"Sent: {order['order_id']} | {order['city']} | €{order['total_amount']}")

        except Exception as e:
            failed += 1
            print(f"Failed: {str(e)}")

    print(f"Summary: Sent={sent} | Failed={failed}")

    return {
        "statusCode": 200,
        "sent": sent,
        "failed": failed
    }