from kafka import KafkaProducer
import json
import time
# Create kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Load json
with open('data/kafka_sample_2025_01_01.json', 'r') as file:
    records = json.load(file)

# Send json to kafka
for row in records:
    producer.send('traffic_topic', value=row)
    print(f"Produced: {row}")
    time.sleep(2)  # Delay in seconds to simulate streaming
