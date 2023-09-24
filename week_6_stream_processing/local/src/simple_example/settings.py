BOOTSTRAP_SERVERS = ['localhost:9092']

TOPIC = 'rides_csv'
PRODUCER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'key_serializer': lambda key: key.encode('utf-8'),
    'value_serializer': lambda value: value.encode('utf-8')
}
CONSUMER_CONFIG = {
    'bootstrap_servers': BOOTSTRAP_SERVERS,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'key_deserializer': lambda key: int(key.decode('utf-8')),
    'value_deserializer': lambda value: value.decode('utf-8'),
    'group_id': 'consumer.group.id.csv-example',
}
