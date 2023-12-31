import io
import os
import requests
import pandas as pd
from prefect import flow, task
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "prefect-de-zoomcamp-hauct")

@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_url, compression='gzip')
    return df

@task(log_prints=True)
def clean(color: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    yellow 2019
    VendorID                   int64
    tpep_pickup_datetime      object (green=lpep_pickup_datetime)
    tpep_dropoff_datetime     object (green=lpep_dropoff_datetime)
    passenger_count            int64
    trip_distance            float64
    RatecodeID                 int64
    store_and_fwd_flag        object
    PULocationID               int64
    DOLocationID               int64
    payment_type               int64
    fare_amount              float64
    extra                    float64
    mta_tax                  float64
    tip_amount               float64
    tolls_amount             float64
    improvement_surcharge    float64
    total_amount             float64
    congestion_surcharge     float64

    yellow 2020
    VendorID                 float64 (2019=int64)
    tpep_pickup_datetime      object (green=lpep_pickup_datetime)
    tpep_dropoff_datetime     object (green=lpep_dropoff_datetime)
    passenger_count          float64 (2019=int64)
    trip_distance            float64
    RatecodeID               float64 (2019=int64)
    store_and_fwd_flag        object
    PULocationID               int64
    DOLocationID               int64
    payment_type             float64 (2019=int64)
    fare_amount              float64
    extra                    float64
    mta_tax                  float64
    tip_amount               float64
    tolls_amount             float64
    improvement_surcharge    float64
    total_amount             float64
    congestion_surcharge     float64

    green 2019
    VendorID                   int64
    lpep_pickup_datetime      object
    lpep_dropoff_datetime     object
    store_and_fwd_flag        object
    RatecodeID                 int64
    PULocationID               int64
    DOLocationID               int64
    passenger_count            int64
    trip_distance            float64
    fare_amount              float64
    extra                    float64
    mta_tax                  float64
    tip_amount               float64
    tolls_amount             float64
    ehail_fee                float64
    improvement_surcharge    float64
    total_amount             float64
    payment_type               int64
    trip_type                  int64
    congestion_surcharge     float64

    green 2020
    VendorID                 float64 (2019=int64)
    lpep_pickup_datetime      object
    lpep_dropoff_datetime     object
    store_and_fwd_flag        object
    RatecodeID               float64 (2019=int64)
    PULocationID               int64
    DOLocationID               int64
    passenger_count          float64 (2019=int64)
    trip_distance            float64
    fare_amount              float64
    extra                    float64
    mta_tax                  float64
    tip_amount               float64
    tolls_amount             float64
    ehail_fee                float64
    improvement_surcharge    float64
    total_amount             float64
    payment_type             float64 (2019=int64)
    trip_type                float64 (2019=int64)
    congestion_surcharge     float64
    """

    if color == "yellow":
        """Fix dtype issues"""
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    if color == "green":
        """Fix dtype issues"""
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        df["trip_type"] = df["trip_type"].astype('Int64')

    if color == "yellow" or color == "green":
        df["VendorID"] = df["VendorID"].astype('Int64')
        df["RatecodeID"] = df["RatecodeID"].astype('Int64')
        df["PULocationID"] = df["PULocationID"].astype('Int64')
        df["DOLocationID"] = df["DOLocationID"].astype('Int64')
        df["passenger_count"] = df["passenger_count"].astype('Int64')
        df["payment_type"] = df["payment_type"].astype('Int64')

    print(f"rows: {len(df)}")
    return df

@task(log_prints=True)
def write_local(color: str, year: str, month:str) -> str:
    file_name = f"{color}_tripdata_{year}-{month}.csv.gz"

    # download it using requests via a pandas df
    request_url = f"{init_url}{color}/{file_name}"
    r = requests.get(request_url)
    open(file_name, 'wb').write(r.content)
    print(f"Local: {file_name}")
    return file_name

@task(log_prints=True)
def upload_to_gcs(bucket: str, object_name: str, local_file: str) -> None:
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    colors = ["yellow", "green"]
    years = ['2019', '2020']

    for color in colors:
        for year in years:
            # Only take first half of a year
            for i in range(6):
                month = '0'+str(i+1)
                month = month[-2:] 

                dataset_url = write_local(color, year, month)

                df = fetch(dataset_url)
                df_clean = clean(color, df)

                dataset_url = dataset_url.replace('.csv.gz', '.parquet')
                df_clean.to_parquet(dataset_url, engine='pyarrow')

                upload_to_gcs(BUCKET, f"data/{color}/{dataset_url}", dataset_url)
                print(f"GCS: {color}/{dataset_url}")

if __name__ == "__main__":
    etl_web_to_gcs()

