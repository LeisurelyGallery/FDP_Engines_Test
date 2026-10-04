import csv
import json
import argparse
from kafka import KafkaProducer
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

csv_file = Path(__file__).parent / "CSV" / "users.csv"

# Command-line arguments
parser = argparse.ArgumentParser(
    description="FDP Engines Kafka CSV Producer"
)

parser.add_argument("--limit",
    type=int,
    default=None,
    help="Number of records to send. If omitted, all records are sent."
)

args = parser.parse_args()

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:39092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Starting FDP Engines Producer...\n")

records_sent = 0

# Read CSV and send records to Kafka
# try/finally ensures that the Kafka producer is flushed and closed even if an error occurs while processing the CSV file.
try:
    with open(csv_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:

            if args.limit is not None and records_sent >= args.limit:
                break

            row["ingested_at"] = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S.%f")

            producer.send("users", value=row)

            records_sent += 1

            if records_sent % 1000 == 0:
                print(f"Sent {records_sent} records...")

finally:
    producer.flush()
    producer.close()

print(
    f"\nAll records sent successfully! Total records sent: {records_sent}"
)