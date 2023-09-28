# Import necessary modules
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

# Import configurations and utility functions from external modules
from settings import PRODUCER_CONFIG, TOPIC
from utils import read_csv

# Define a custom Kafka producer class that inherits from KafkaProducer
class SimpleProducer(KafkaProducer):
    def __init__(self, props: Dict):
        # Initialize the KafkaProducer with the provided properties
        self.kafka_producer = KafkaProducer(**props)

    def publish_messages(self, topic: str, messages: List[str]):
        # Iterate over the list of messages to be published
        for message in messages:
            try:
                # Send a message to the specified Kafka topic
                record = self.kafka_producer.send(key=message[0], value=message, topic=topic)
                # Print a success message with the produced message and its offset
                print('Record:{} successfully produced at offset:{}'
                      .format(message, record.get().offset))
            except KafkaTimeoutError as e:
                # Handle KafkaTimeoutError by printing an error message
                print(e.__str__())

# Entry point of the script
if __name__ == '__main__':
    # Read CSV data from the 'read_csv' utility function
    rides = read_csv()

    # Create an instance of the SimpleProducer class with the provided producer configuration
    producer = SimpleProducer(props=PRODUCER_CONFIG)
    
    # Publish the read CSV data as messages to the specified Kafka topic
    producer.publish_messages(topic=TOPIC, messages=rides)
