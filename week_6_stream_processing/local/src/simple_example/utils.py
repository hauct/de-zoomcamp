import csv
from typing import List

INPUT_DATA_PATH = './resources/data/rides.csv'


def read_csv(resource_path: str = INPUT_DATA_PATH) -> List[str]:
    records = []
    with open(resource_path, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip the header
        for row in reader:
            records.append(', '.join(row))
        return records
