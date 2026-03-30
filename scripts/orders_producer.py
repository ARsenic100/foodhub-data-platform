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