from typing import Dict, List

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField, IntegerDeserializer

from ride import dict_to_ride
from settings import CONSUMER_CONFIG, TOPIC
from utils import load_schema


class RideAvroConsumer:
    def __init__(self, props: Dict):

        # Schemas & Schema Registry Configuration
        value_schema_str = load_schema(props['schema.value'])
        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)
        # Deserializers
        self.avro_key_deserializer = IntegerDeserializer()
        self.avro_value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                                        schema_str=value_schema_str, from_dict=dict_to_ride)

        # Consumer Configuration
        consumer_props = {'bootstrap.servers': props['bootstrap.servers'],
                          'group.id': 'taxi.rides.kafka.avro',
                          'auto.offset.reset': "earliest"}
        self.consumer = Consumer(consumer_props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                message_key = self.avro_key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                message_value = self.avro_value_deserializer(msg.value(),
                                                             SerializationContext(msg.topic(), MessageField.VALUE))
                if message_value is not None:
                    print("{}, {}".format(message_key, message_value))
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__":
    avro_consumer = RideAvroConsumer(props=CONSUMER_CONFIG)
    avro_consumer.consume_from_kafka(topics=[TOPIC])
