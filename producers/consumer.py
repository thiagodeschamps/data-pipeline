import json 
from kafka import KafkaConsumer

# Kafka Consumer 
consumer = KafkaConsumer(
    'messages',
    bootstrap_servers='host.docker.internal:9094',
    auto_offset_reset='earliest'
)

for message in consumer:
    print(json.loads(message.value))