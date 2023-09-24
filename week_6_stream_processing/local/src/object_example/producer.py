import argparse
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from settings import PRODUCER_CONFIG, TOPIC
from ride import Ride
from utils import read_rides


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.kafka_producer = KafkaProducer(**props)

    def publish_messages(self, topic: str, messages: List[Ride]):
        for message in messages:
            try:
                if message.__class__.__name__ == 'Ride':
                    record = self.kafka_producer.send(key=message.pu_location_id, value=message, topic=topic)
                    print('Key-pu_location_id:{} successfully produced at offset:{}' \
                          .format(message.pu_location_id, record.get().offset))
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == '__main__':
    rides = read_rides()

    producer = JsonProducer(props=PRODUCER_CONFIG)
    producer.publish_messages(topic=TOPIC, messages=rides)
