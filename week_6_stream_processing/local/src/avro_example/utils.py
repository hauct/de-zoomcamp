import csv
import os
from typing import List

from ride import Ride

INPUT_DATA_PATH = './resources/data/rides.csv'


def read_rides(resource_path: str = INPUT_DATA_PATH) -> List[Ride]:
    rides = []
    with open(resource_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip the header row
        for row in reader:
            rides.append(Ride(arr=row))
    return rides


def load_schema(schema_path: str):
    path = os.path.realpath(os.path.dirname(__file__))
    with open(f"{path}/{schema_path}") as f:
        schema_str = f.read()
    return schema_str


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record:{} successfully produced to topic:{} partition:[{}] at offset:{}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
