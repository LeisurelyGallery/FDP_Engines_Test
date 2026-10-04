"""Publish both input CSVs to Kafka for the Flink consolidation job."""
import argparse
import csv
import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from kafka import KafkaProducer

ROOT = Path(__file__).parent
parser = argparse.ArgumentParser()
parser.add_argument('--bootstrap-server', default='localhost:39092')
parser.add_argument('--limit', type=int, default=None)
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap_server,
    value_serializer=lambda value: json.dumps(value).encode('utf-8'),
    acks='all', retries=5, compression_type='gzip')

try:
    for filename, topic in [('users.csv', 'users'), ('Test.csv', 'Test')]:
        with open(ROOT / 'CSV' / filename, encoding='utf-8', newline='') as source:
            for index, row in enumerate(csv.DictReader(source)):
                if args.limit is not None and index >= args.limit:
                    break
                row['ingested_at'] = datetime.now(ZoneInfo('Asia/Kolkata')).isoformat()
                producer.send(topic, row)
        print(f'Published {filename} to topic {topic}')
finally:
    producer.flush()
    producer.close()
