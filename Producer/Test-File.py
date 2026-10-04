import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Defaults
DEFAULT_BOOTSTRAP_SERVER = "localhost:39092"
DEFAULT_TOPIC = "Test"
DEFAULT_CSV_FILE = Path(__file__).parent / "CSV" / "Test.csv"
DEFAULT_TIMEZONE = "Asia/Kolkata"

DEFAULT_MODE = "burst"
DEFAULT_RATE = 1

PRODUCER_ACKS = "all"
PRODUCER_RETRIES = 5

# Command-line arguments
parser = argparse.ArgumentParser(
    description="FDP Engines Kafka CSV Producer"
)

parser.add_argument(
    "--file",
    type=Path,
    default=DEFAULT_CSV_FILE,
    help="CSV file to read."
)

parser.add_argument(
    "--topic",
    default=DEFAULT_TOPIC,
    help="Kafka topic to publish to."
)

parser.add_argument(
    "--bootstrap-server",
    default=DEFAULT_BOOTSTRAP_SERVER,
    help="Kafka bootstrap server."
)

parser.add_argument(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to send. If omitted, all records are sent."
)

parser.add_argument(
    "--mode",
    choices=["burst", "stream"],
    default=DEFAULT_MODE,
    help="burst = send as fast as possible; stream = controlled rate."
)

parser.add_argument(
    "--rate",
    type=float,
    default=DEFAULT_RATE,
    help="Records per second in stream mode."
)

args = parser.parse_args()

# Input validation
if args.limit is not None and args.limit <= 0:
    parser.error("--limit must be greater than 0.")

if args.mode == "stream" and args.rate <= 0:
    parser.error("--rate must be greater than 0.")

if not args.file.exists():
    parser.error(f"CSV file not found: {args.file}")

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=args.bootstrap_server,

    value_serializer=lambda value:
        json.dumps(value).encode("utf-8"),

    # Reliability
    acks=PRODUCER_ACKS,
    retries=PRODUCER_RETRIES,

    # Allow Kafka producer batching
    linger_ms=10,

    # Compression improves network efficiency
    compression_type="gzip"
)

# Runtime statistics
records_submitted = 0
records_succeeded = 0
records_failed = 0

start_time = time.perf_counter()

# Delivery callback
def delivery_callback(record_metadata):
    global records_succeeded

    records_succeeded += 1

def delivery_error(record, exception):
    global records_failed

    records_failed += 1

    print(
        f"\n[ERROR] Kafka delivery failed: "
        f"topic={args.topic}, "
        f"record={record}, "
        f"error={exception}"
    )

# Producer execution
print("Starting FDP Engines Producer...\n")

print(f"CSV File        : {args.file}")
print(f"Kafka Server    : {args.bootstrap_server}")
print(f"Kafka Topic     : {args.topic}")
print(f"Mode            : {args.mode}")

if args.limit is not None:
    print(f"Record Limit    : {args.limit}")
else:
    print("Record Limit    : All records")

if args.mode == "stream":
    print(f"Stream Rate     : {args.rate} records/sec")

print()

try:

    with open(args.file, "r", encoding="utf-8", newline="") as file:

        reader = csv.DictReader(file)

        # Validate CSV
        if reader.fieldnames is None:
            raise ValueError("CSV file does not contain a header row.")

        required_columns = {
            "id",
            "firstName",
            "lastName",
            "email",
            "username",
            "phone",
            "zip",
            "gender",
        }

        missing_columns = required_columns - set(reader.fieldnames)

        if missing_columns:
            raise ValueError(
                f"CSV is missing required columns: "
                f"{', '.join(sorted(missing_columns))}"
            )

        # Read and send records
        for row in reader:

            if (
                args.limit is not None
                and records_submitted >= args.limit
            ):
                break

            # Generate IST ingestion timestamp
            row["ingested_at"] = datetime.now(
                ZoneInfo(DEFAULT_TIMEZONE)
            ).strftime("%Y-%m-%d %H:%M:%S.%f")

            # Submit asynchronously to Kafka
            future = producer.send(
                args.topic,
                value=row
            )

            records_submitted += 1

            # Register delivery callback
            future.add_callback(delivery_callback)
            future.add_errback(
                lambda exception, record=row:
                    delivery_error(record, exception)
            )

            # Stream mode
            if args.mode == "stream":
                time.sleep(1 / args.rate)

            # Progress
            if records_submitted % 1000 == 0:
                print(
                    f"Submitted {records_submitted} records..."
                )

finally:

    # Wait for all outstanding Kafka messages
    producer.flush()
    producer.close()

# Final statistics
elapsed_time = time.perf_counter() - start_time

if elapsed_time > 0:
    throughput = records_succeeded / elapsed_time
else:
    throughput = 0

print("\n----------------------------------------")
print("FDP Engines Producer Summary")
print("----------------------------------------")

print(f"Records submitted : {records_submitted}")
print(f"Records succeeded : {records_succeeded}")
print(f"Records failed    : {records_failed}")
print(f"Elapsed time      : {elapsed_time:.2f} seconds")
print(f"Throughput        : {throughput:.2f} records/sec")

if records_failed == 0:
    print("\nStatus            : SUCCESS")
else:
    print("\nStatus            : COMPLETED WITH ERRORS")