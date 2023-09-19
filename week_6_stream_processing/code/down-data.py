import requests
import gzip
import os

def download_and_extract(url):
    response = requests.get(url, stream=True)
    with gzip.open(response.raw, 'rb') as gz:
        # Get the name of files
        output_filename = os.path.basename(url)
        output_filename_without_extension = os.path.splitext(output_filename)[0]
        
        with open(output_filename_without_extension, 'wb') as out_file:
            out_file.write(gz.read())

GREEN_TRIP_DATA_URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz'
FHV_TRIP_DATA_PATH_URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz' 

print('Downloading data')

download_and_extract(GREEN_TRIP_DATA_URL)
download_and_extract(FHV_TRIP_DATA_PATH_URL)

print('Download Completed !!!')