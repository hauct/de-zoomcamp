from typing import Dict, List
from confluent_kafka import Consumer

from settings import CONFLUENT_CLOUD_CONFIG, TOPIC


class SimpleConsumer:
    def __init__(self, props: Dict):
        self.consumer = Consumer(**props)

    def consume_from_topics(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.key is not None:
                    print("Key:{}, Value:{}".format(msg.key(), msg.value()))
            except KeyboardInterrupt:
                break
        self.consumer.close()

if __name__ == '__main__':

    consumer_characteristics = {
        'group.id': 'taxirides.csv.consumer',
        'auto.offset.reset': "earliest"
    }
    props = {**CONFLUENT_CLOUD_CONFIG, **consumer_characteristics}

    simple_consumer = SimpleConsumer(props=props)
    simple_consumer.consume_from_topics(topics=[TOPIC])