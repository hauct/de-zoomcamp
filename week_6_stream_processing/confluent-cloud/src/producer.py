from typing import List, Dict
from time import sleep
from confluent_kafka import Producer

from settings import CONFLUENT_CLOUD_CONFIG, TOPIC
from utils import read_csv, delivery_report

class SimpleProducer(Producer):
    def __init__(self, props: Dict):
        self.producer = Producer(**props)
    @staticmethod
    def parse_message(message:str):
        key, value  = message.split(', ')[0], message
        return key, value

    def publish_messages(self, topic: str, messages: List[str]):
        for message in messages:
            try:
                msg_key, msg_value = self.parse_message(message)
                self.producer.produce(key=msg_key, value=msg_value, topic=topic, on_delivery=delivery_report)
            except BufferError as bfer:
                # Polls the producer for events and calls the corresponding callbacks (if registered).
                self.producer.poll(0.1) 
            except Exception as e:
                print(f"Exception while producing message - {message}: {e}")
            except KeyboardInterrupt:
                break

        # Wait for all messages in the Producer queue to be delivered
        self.producer.flush() 
        sleep(10)

if __name__ == '__main__':
    rides = read_csv()

    producer = SimpleProducer(props=CONFLUENT_CLOUD_CONFIG)
    producer.publish_messages(topic=TOPIC, messages=rides)