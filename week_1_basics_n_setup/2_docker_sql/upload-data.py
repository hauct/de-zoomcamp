#!/usr/bin/env python
# coding: utf-8
import os
import argparse

from time import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    # Import the params from the input
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name

    # Create engine to connect to pg db
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Download the file from Google Drive
    url_code = '1p2y79m_q9rPM48cEYwQdrk6XVCTFSyRA'
    os.system(f'gdown {url_code}')

    # Read the file csv, split into multiple 100000 rows files
    csv_name = 'yellow_tripdata_2021-01.csv'
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    # Iterate
    df = next(df_iter)

    # Convert time
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    # Export data into database, first we get the columns names
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Export data
    df.to_sql(name=table_name, con=engine, if_exists='append')

    # Export the rest
    while True:
        try:            
            t_start = time()

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')

    args = parser.parse_args()

    main(args)

