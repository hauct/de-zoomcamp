RIDE_SCHEMA_PATH = '../../resources/schemas/ride.avsc'
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC = 'rides_avro'
PRODUCER_CONFIG = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'schema_registry.url': SCHEMA_REGISTRY_URL,
    'schema.value': RIDE_SCHEMA_PATH
}
CONSUMER_CONFIG = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'schema_registry.url': SCHEMA_REGISTRY_URL,
    'schema.value': RIDE_SCHEMA_PATH,
}
