# Define the Kafka server(s) that the producer and consumer will connect to
BOOTSTRAP_SERVERS = ['localhost:9092']

# Define the Kafka topic that the producer and consumer will work with
TOPIC = 'rides_csv'

# Configuration settings for the Kafka producer
PRODUCER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,  # List of Kafka broker(s) to connect to
    'key_serializer': lambda key: key.encode('utf-8'),  # Serializer for message keys
    'value_serializer': lambda value: value.encode('utf-8')  # Serializer for message values
}

# Configuration settings for the Kafka consumer
CONSUMER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,  # List of Kafka broker(s) to connect to
    'auto_offset_reset': 'earliest',  # Set the offset to the earliest available message when the consumer starts
    'enable_auto_commit': True,  # Enable automatic offset committing by the consumer
    'key_deserializer': lambda key: int(key.decode('utf-8')),  # Deserializer for message keys
    'value_deserializer': lambda value: value.decode('utf-8'),  # Deserializer for message values
    'group_id': 'consumer.group.id.csv-example'  # Unique identifier for the consumer group
}
