import os
import csv
from typing import List


# Get the path of file csv
CURRENT_FILE_PATH = os.path.abspath(__file__)
SRC_DIR = os.path.dirname(CURRENT_FILE_PATH)
BASE_DIR = os.path.dirname(SRC_DIR)

INPUT_DATA_PATH = os.path.join(BASE_DIR, 'resources', 'data', 'rides.csv')

# Read csv, convert to a list
def read_csv(resource_path: str = INPUT_DATA_PATH) -> List[str]:
    records = []
    with open(resource_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip the header
        for row in reader:
            records.append(', '.join(row))
        return records
    
# Report delivery status
def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record:{} successfully produced to topic:{} partition:[{}] at offset:{}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))