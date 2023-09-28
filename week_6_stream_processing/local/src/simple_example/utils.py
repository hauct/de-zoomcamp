import csv
from typing import List
import os

# Define the path to the CSV file
CURRENT_FILE_PATH = os.getcwd()
SRC_DIR = os.path.dirname(CURRENT_FILE_PATH)
LOCAL_DIR = os.path.dirname(SRC_DIR)
INPUT_DATA_PATH = os.path.join(LOCAL_DIR, 'resources', 'data', 'rides.csv')

# Function to read and parse ride data from a CSV file
def read_csv(resource_path: str = INPUT_DATA_PATH) -> List[str]:
    records = [] # Create an empty list to store row
    with open(resource_path, 'r') as f:
        reader = csv.reader(f) # Create a CSV reader for the file
        header = next(reader)  # skip the header
        for row in reader:  # Iterate over each row in the CSV file
            records.append(', '.join(row)) # Add string
        return records  # Return the list of string