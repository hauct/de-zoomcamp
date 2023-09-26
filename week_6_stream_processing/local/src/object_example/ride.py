from typing import List, Dict
from decimal import Decimal
from datetime import datetime

# Define the Ride class
class Ride:
    # Initialize a Ride object from a list of input values
    def __init__(self, arr: List[str]):
        self.vendor_id = int(arr[0])  # Vendor ID
        self.tpep_pickup_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S")  # Pickup datetime
        self.tpep_dropoff_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S")  # Dropoff datetime
        self.passenger_count = int(arr[3])  # Passenger count
        self.trip_distance = Decimal(arr[4])  # Trip distance
        self.rate_code_id = int(arr[5])  # Rate code ID
        self.store_and_fwd_flag = arr[6]  # Store and forward flag
        self.pu_location_id = int(arr[7])  # Pickup location ID
        self.do_location_id = int(arr[8])  # Dropoff location ID
        self.payment_type = arr[9]  # Payment type
        self.fare_amount = Decimal(arr[10])  # Fare amount
        self.extra = Decimal(arr[11])  # Extra charges
        self.mta_tax = Decimal(arr[12])  # MTA tax
        self.tip_amount = Decimal(arr[13])  # Tip amount
        self.tolls_amount = Decimal(arr[14])  # Tolls amount
        self.improvement_surcharge = Decimal(arr[15])  # Improvement surcharge
        self.total_amount = Decimal(arr[16])  # Total amount
        self.congestion_surcharge = Decimal(arr[17])  # Congestion surcharge

    # Class method to create a Ride object from a dictionary
    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['vendor_id'],
            d['tpep_pickup_datetime'],
            d['tpep_dropoff_datetime'],
            d['passenger_count'],
            d['trip_distance'],
            d['rate_code_id'],
            d['store_and_fwd_flag'],
            d['pu_location_id'],
            d['do_location_id'],
            d['payment_type'],
            d['fare_amount'],
            d['extra'],
            d['mta_tax'],
            d['tip_amount'],
            d['tolls_amount'],
            d['improvement_surcharge'],
            d['total_amount'],
            d['congestion_surcharge'],
        ])
        
    # Method to represent the object as a string
    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'

# Function ride_to_dict to convert a Ride object to a dictionary
def ride_to_dict(ride: Ride, ctx):
    return ride.__dict__