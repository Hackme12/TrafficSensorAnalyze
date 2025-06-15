#!/bin/bash

# Activate Python environment if needed (uncomment and modify if using virtualenv or conda)
# source /path/to/venv/bin/activate
# conda activate your_env_name

echo "Starting Kafka Producer..."
python kafka_producer.py &   # Run in background

sleep 3  # Give the producer a head start

echo "Starting Kafka Traffic Sensor Consumer..."
python kafka_traffic_sensor.py





echo "Starting Kafka Traffic Sensor Consumer..."
python traffic_sensor.py

# Optional: Wait for background processes to finish if needed
wait

echo "Kafka streaming pipeline completed."