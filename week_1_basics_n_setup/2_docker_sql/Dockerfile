FROM python:3.9.1
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY upload-data.py upload-data.py
COPY yellow_tripdata_2021-01.csv yellow_tripdata_2021-01.csv

ENTRYPOINT [ "python", "upload-data.py", \
             "--user", "root", \
             "--password", "root", \
             "--host", "192.168.176.1", \
             "--port", "5432", \
             "--db", "ny_taxi", \
             "--table_name", "yellow_taxi_data"]
