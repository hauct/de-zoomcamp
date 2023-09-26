import json
from ride import Ride

# Kafka server configuration
BOOTSTRAP_SERVERS = ['localhost:9092']

# Kafka topic for sending and receiving JSON data
TOPIC = 'rides_json'

# Configuration for Kafka producer
PRODUCER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'key_serializer': lambda key: str(key).encode(),
    'value_serializer': lambda value: json.dumps(value.__dict__, default=str).encode('utf-8')
}

# Configuration for Kafka consumer
CONSUMER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'key_deserializer': lambda key: int(key.decode('utf-8')),
    'value_deserializer': lambda value: json.loads(value.decode('utf-8'), object_hook=lambda d: Ride.from_dict(d)),
    'group_id': 'simple.kafka.json.example',
}