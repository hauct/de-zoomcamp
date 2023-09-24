import argparse
from typing import Dict, List
from kafka import KafkaConsumer

from settings import CONSUMER_CONFIG, TOPIC


class SimpleConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_topics(self, topics: List[str]):
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                state = self.consumer.poll(1.0)  # dict{topic1 = [()], topic2=[(),]}
                if state is None or state == {}:
                    continue
                for topic, records in state.items():
                    for record in records:
                        print(f'Key: {record.key} || Value:{record.value} || {topic}')
            except KeyboardInterrupt:
                break
        self.consumer.close()


if __name__ == '__main__':
    simple_consumer = SimpleConsumer(props=CONSUMER_CONFIG)
    simple_consumer.consume_from_topics(topics=[TOPIC])
