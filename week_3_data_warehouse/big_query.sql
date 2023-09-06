-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-alexey-396910.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://prefect-de-zoomcamp-hauct/yellow/*.parquet'
  ]
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE ny-rides-alexey-396910.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM ny-rides-alexey-396910.nytaxi.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ny-rides-alexey-396910.nytaxi.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM ny-rides-alexey-396910.nytaxi.external_yellow_tripdata;

-- Impact of partition
SELECT DISTINCT(VendorID)
FROM ny-rides-alexey-396910.nytaxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT DISTINCT(VendorID)
FROM ny-rides-alexey-396910.nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
