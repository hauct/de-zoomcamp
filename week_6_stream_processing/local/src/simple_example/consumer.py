import argparse
from typing import Dict, List
from kafka import KafkaConsumer

# Import configurations and topic from external module
from settings import CONSUMER_CONFIG, TOPIC

# Define a SimpleConsumer class for Kafka data consumption
class SimpleConsumer:
    def __init__(self, props: Dict):
        # Initialize a KafkaConsumer instance with provided properties
        self.consumer = KafkaConsumer(**props)

    def consume_from_topics(self, topics: List[str]):
        # Subscribe to the specified Kafka topics
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # Poll for new Kafka messages with a timeout of 1 second
                state = self.consumer.poll(1.0)  # dict{topic1 = [()], topic2=[(),]}
                if state is None or state == {}:
                    continue
                for topic, records in state.items():
                    for record in records:
                        # Print the key, value, and topic of each Kafka record
                        print(f'Key: {record.key} || Value:{record.value} || {topic}')
            except KeyboardInterrupt:
                break
        # Close the KafkaConsumer when finished
        self.consumer.close()

if __name__ == '__main__':
    # Create an instance of SimpleConsumer with consumer configuration
    simple_consumer = SimpleConsumer(props=CONSUMER_CONFIG)
    
    # Consume data from the specified Kafka topic
    simple_consumer.consume_from_topics(topics=[TOPIC])