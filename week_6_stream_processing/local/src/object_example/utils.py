import csv
from typing import List
from ride import Ride  
import os

# Define the path to the CSV file
CURRENT_FILE_PATH = os.getcwd()
SRC_DIR = os.path.dirname(CURRENT_FILE_PATH)
LOCAL_DIR = os.path.dirname(SRC_DIR)
INPUT_DATA_PATH = os.path.join(LOCAL_DIR, 'resources', 'data', 'rides.csv')

# Function to read and parse ride data from a CSV file
def read_rides(resource_path: str = INPUT_DATA_PATH) -> List[Ride]:
    rides = []  # Create an empty list to store Ride objects
    with open(resource_path, 'r') as f:
        reader = csv.reader(f)  # Create a CSV reader for the file
        header = next(reader)  # Skip the header row (assuming it contains column names)
        for row in reader:  # Iterate over each row in the CSV file
            rides.append(Ride(arr=row))  # Create a Ride object from the row and add it to the list
    return rides  # Return the list of Ride objects
