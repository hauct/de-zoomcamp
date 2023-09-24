import csv
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
