import io
import os
import requests
import pandas as pd
from google.cloud import storage


init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

service = 'yellow'
year = '2019'
month = '02'

file_name  = f"{service}_tripdata_{year}-{month}.csv.gz"

request_url = f"{init_url}{service}/{file_name}"
r = requests.get(request_url)
open(file_name, 'wb').write(r.content)
print(f"Local: {file_name}")

df = pd.read_csv(file_name, compression='gzip')

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

df = clean(df)

print(df.info())