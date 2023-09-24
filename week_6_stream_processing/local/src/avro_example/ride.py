from typing import List, Dict
from decimal import Decimal
from datetime import datetime

import fastavro


def encode_datetime_as_string(data, *args):
    return data.strftime("%Y-%m-%d %H:%M:%S")


def decode_string_as_datetime(data, *args):
    return datetime.strptime(data, "%Y-%m-%d %H:%M:%S"),


fastavro.write.LOGICAL_WRITERS["string-customdatetime"] = encode_datetime_as_string
fastavro.read.LOGICAL_READERS["string-customdatetime"] = decode_string_as_datetime


# ["null", {"type": "string", "logicalType": "datetime2",},]
class Ride:
    def __init__(self, arr: List[str]):
        self.vendor_id = int(arr[0])
        self.tpep_pickup_datetime = arr[1]  # TODO: Modify schema to support datetime datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        self.tpep_dropoff_datetime = arr[2]  # TODO: Modify schema to support datetime datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        self.passenger_count = int(arr[3])
        self.trip_distance = Decimal(arr[4])
        self.rate_code_id = int(arr[5])
        self.store_and_fwd_flag = arr[6]
        self.pu_location_id = int(arr[7])
        self.do_location_id = int(arr[8])
        self.payment_type = arr[9]
        self.fare_amount = Decimal(arr[10])
        self.extra = Decimal(arr[11])
        self.mta_tax = Decimal(arr[12])
        self.tip_amount = Decimal(arr[13])
        self.tolls_amount = Decimal(arr[14])
        self.improvement_surcharge = Decimal(arr[15])
        self.total_amount = Decimal(arr[16])
        self.congestion_surcharge = Decimal(arr[17])

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
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'


def dict_to_ride(obj, ctx):
    if obj is None:
        return None
    return Ride.from_dict(obj)


def ride_to_dict(ride: Ride, ctx):
    return ride.__dict__
