from time import sleep
from typing import Dict, List

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import IntegerSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride import Ride, ride_to_dict

from settings import TOPIC, PRODUCER_CONFIG
from utils import read_rides, delivery_report, load_schema


class RideAvroProducer:
    def __init__(self, props: Dict):
        # Schemas & Schema Registry Configuration
        value_schema_str = load_schema(props['schema.value'])
        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)
        # Serializers
        self.key_serializer = IntegerSerializer()
        self.value_serializer = AvroSerializer(schema_registry_client, value_schema_str, ride_to_dict)

        # Producer Configuration
        producer_props = {'bootstrap.servers': props['bootstrap.servers']}
        self.producer = Producer(producer_props)

    def publish(self, topic: str, records: List[Ride]):
        for record in records:
            try:
                self.producer.produce(
                    key=self.key_serializer(record.vendor_id, SerializationContext(topic=topic, field=MessageField.KEY)),
                    value=self.value_serializer(record, SerializationContext(topic=topic, field=MessageField.VALUE)),
                    topic=topic,
                    on_delivery=delivery_report)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {record}: {e}")

        self.producer.flush()
        sleep(1)


if __name__ == "__main__":
    rides = read_rides()

    producer = RideAvroProducer(props=PRODUCER_CONFIG)
    producer.publish(topic=TOPIC, records=rides)
