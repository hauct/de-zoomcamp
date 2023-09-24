from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from settings import PRODUCER_CONFIG, TOPIC
from utils import read_csv


class SimpleProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.kafka_producer = KafkaProducer(**props)

    def publish_messages(self, topic: str, messages: List[str]):
        for message in messages:
            try:
                record = self.kafka_producer.send(key=message[0], value=message, topic=topic)
                print('Record:{} successfully produced at offset:{}' \
                      .format(message, record.get().offset))
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == '__main__':
    rides = read_csv()

    producer = SimpleProducer(props=PRODUCER_CONFIG)
    producer.publish_messages(topic=TOPIC, messages=rides)
