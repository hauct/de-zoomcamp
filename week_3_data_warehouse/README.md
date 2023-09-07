# Week 3: Data Warehouse and BigQuery

See [week_3\_data_warehouse](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_3_data_warehouse)
on GitHub.

Youtube videos:

- [DE Zoomcamp 3.1.1 - Data Warehouse and BigQuery](https://www.youtube.com/watch?v=jrHljAoD6nM)
- [DE Zoomcamp 3.1.2 - Partioning and Clustering](https://www.youtube.com/watch?v=-CqXf7vhhDs)
- [DE Zoomcamp 3.2.1 - BigQuery Best Practices](https://www.youtube.com/watch?v=k81mLJVX08w)
- [DE Zoomcamp 3.2.2 - Internals of BigQuery](https://www.youtube.com/watch?v=eduHi1inM4s)
- [DE Zoomcamp 3.3.1 - BigQuery Machine Learning](https://www.youtube.com/watch?v=B-WtpB0PuG4)
- [DE Zoomcamp 3.3.2 - BigQuery Machine Learning Deployment](https://www.youtube.com/watch?v=BjARzEWaznU)

Basic SQL:

- [BigQuery](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query.sql)
- [External
  Table](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query_hw.sql)
- [BigQuery
  ML](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/big_query_ml.sql)

## Concepts

### OLAP vs OLTP

**OLTP** systems are designed to handle large volumes of transactional data involving multiple users.

**OLAP** system is designed to process large amounts of data quickly, allowing users to analyze multiple data dimensions
in tandem. Teams can use this data for decision-making and problem-solving.

|   | **OLTP**  | **OLAP**  |
|---------------------|---------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| Purpose | Control and run essential business operations in real time  | Plan, solve problems, support decisions, discover hidden insights |
| Data updates  | Short, fast updates initiated by user | Data periodically refreshed with scheduled, long-running batch jobs |
| Database design | Normalized databases for efficiency | Denormalized databases for analysis |
| Space requirements  | Generally small if historical data is archived  | Generally large due to aggregating large datasets |
| Backup and recovery | Regular backups required to ensure business continuity and meet legal and governance requirements | Lost data can be reloaded from OLTP database as needed in lieu of regular backups |
| Productivity  | Increases productivity of end users | Increases productivity of business managers, data analysts, and executives  |
| Data view | Lists day-to-day business transactions  | Multi-dimensional view of enterprise data |
| User examples | Customer-facing personnel, clerks, online shoppers  | Knowledge workers such as data analysts, business analysts, and executives  |

### What is a data wahehouse

- OLAP solution
- Used for reporting and data analysis

See also [The Data Warehouse Toolkit 3rd
Edition](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/books/data-warehouse-dw-toolkit/)
by Ralph Kimball and Margy Ross.

## BigQuery

### Overwiew

BigQuery is a fully managed enterprise data warehouse that helps you manage and analyze your data with built-in features
like machine learning, geospatial analysis, and business intelligence.

- Serverless data warehouse
  - There are no servers to manage or database software to install
- Software as well as infrastructure including
  - **scalability** and **high-availability**
- Built-in features like
  - machine learning
  - geospatial analysis
  - business intelligence
- BigQuery maximizes flexibility by separating the compute engine that analyzes your data from your storage
- On demand pricing
  - 1 TB of data processed is \$5
- Flat rate pricing
  - Based on number of pre requested slots
  - 100 slots → \$2,000/month = 400 TB data processed on demand pricing

See [What is BigQuery?](https://cloud.google.com/bigquery/docs/introduction) for more information.

### Query Settings

Click on MORE button, select Query Settings. In the Resource management section, we should disable Use cached results.

![p72](images/bq-query-settings.png)

To create the table, you will need to download and upload your data from local to your Google Bucket.
Run the script `web_to_gcs.py`.
``` bash
python web_to_gcs.py
```

<div class="formalpara-title">

**File `web_to_gcs.py`**

</div>

``` python
import io
import os
import requests
import pandas as pd
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


def upload_to_gcs(bucket, object_name, local_file):
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

def web_to_gcs(year, service):
    # We just need first half of a year, which is from January to June
    for i in range(6):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip')
        ### Fix dtype issues ###
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        
        # upload it to gcs
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'yellow')
```

**IMPORTANT NOTE**: You will need to edit your own GCP_GCS_BUCKET to have the data correctly input

``` text
$ python web_to_gcs.py
Local: yellow_tripdata_2019-01.csv.gz
GCS: yellow/yellow_tripdata_2019-01.parquet
Local: yellow_tripdata_2019-02.csv.gz
GCS: yellow/yellow_tripdata_2019-02.parquet
Local: yellow_tripdata_2019-03.csv.gz
GCS: yellow/yellow_tripdata_2019-03.parquet
Local: yellow_tripdata_2019-04.csv.gz
GCS: yellow/yellow_tripdata_2019-04.parquet
Local: yellow_tripdata_2019-05.csv.gz
GCS: yellow/yellow_tripdata_2019-05.parquet
Local: yellow_tripdata_2019-06.csv.gz
GCS: yellow/yellow_tripdata_2019-06.parquet
```

After the process is done, Go to `Big Query`, run the SQL scrip in file `big_query.sql` to create the table `external_yellow_tripdata` , on dataset `nytaxi` of project `ny-rides-alexey-396910`.

**File `big_query.sql`**

``` sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny-rides-alexey-396910.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://prefect-de-zoomcamp-hauct/yellow/*.parquet'
  ]
);
```

![p73](images/bq-create-dataset.png)

We will have our dataset created from now on

![p74](images/bq-dataset.png)

### BigQuery Partition

A partitioned table is divided into segments, called partitions, that make it easier to manage and query your data. By
dividing a large table into smaller partitions, you can improve query performance and control costs by reducing the
number of bytes read by a query. You partition tables by specifying a partition column which is used to segment the
table.

- Time-unit column
- Ingestion time (`_PARTITIONTIME`)
- Integer range partitioning
- When using Time unit or ingestion time
  - Daily (Default)
  - Hourly
  - Monthly or yearly
- Number of partitions limit is 4000

See [Introduction to partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables) for more
information.

**Partition in BigQuery**

![p75](images/bq-partition.png)

We use this SQL code to create two type of table, `yellow_tripdata_non_partitoned` and `yellow_tripdata_partitoned`

**File `big_query.sql`**

``` sql
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE ny-rides-alexey-396910.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM ny-rides-alexey-396910.nytaxi.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ny-rides-alexey-396910.nytaxi.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM ny-rides-alexey-396910.nytaxi.external_yellow_tripdata;
```

And now, i will try to check the peformance of one same query on two tables

```sql
SELECT DISTINCT(VendorID)
FROM ny-rides-alexey-396910.nytaxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT DISTINCT(VendorID)
FROM ny-rides-alexey-396910.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';
```

The result on partitioned table is faster than on non_partitioned one

|                                           |                                          |
|-------------------------------------------|------------------------------------------|
| ![p76](images/bq-non-partitions-query.png)| ![p77](images/bq-partitions-query.png)   |

### BigQuery Clustering

Clustered tables in BigQuery are tables that have a user-defined column sort order using clustered columns. Clustered
tables can improve query performance and reduce query costs.

In BigQuery, a clustered column is a user-defined table property that sorts storage blocks based on the values in the
clustered columns. The storage blocks are adaptively sized based on the size of the table. A clustered table maintains
the sort properties in the context of each operation that modifies it. Queries that filter or aggregate by the clustered
columns only scan the relevant blocks based on the clustered columns instead of the entire table or table partition. As
a result, BigQuery might not be able to accurately estimate the bytes to be processed by the query or the query costs,
but it attempts to reduce the total bytes at execution.

See [Introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables).

- Columns you specify are used to colocate related data
- Order of the column is important
- The order of the specified columns determines the sort order of the data.
- Clustering improves
  - Filter queries
  - Aggregate queries
- Table with data size \< 1 GB, don’t show significant improvement with partitioning and clustering
- You can specify up to four clustering columns

Clustering columns must be top-level, non-repeated columns:

- `DATE`
- `BOOL`
- `GEOGRAPHY`
- `INT64`
- `NUMERIC`
- `BIGNUMERIC`
- `STRING`
- `TIMESTAMP`
- `DATETIME`

**Clustering in BigQuery**

![p78](images/bq-clustering.png)

We will create `yellow_tripdata_partitoned_clustered`, which has a cluster is `VendorID`

```sql
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ny-rides-alexey-396910.nytaxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM ny-rides-alexey-396910.nytaxi.external_yellow_tripdata;
```

And now, we will try to check the peformance of the clusted table and partitioned one above, in one same query

```sql
SELECT count(*) as trips
FROM ny-rides-alexey-396910.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30'
  AND VendorID=1;

SELECT count(*) as trips
FROM ny-rides-alexey-396910.nytaxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30'
  AND VendorID=1;
```

The result is not quite a difference, maybe it's because the size of our table is < 1Gb

|                                  |                                          |
|----------------------------------|------------------------------------------|
| ![p78](images/bq-partition-1.png)| ![p79](images/bq-clustering.png-1.png)   |


### Partitioning vs Clustering


| **Clustering**  | **Partitoning** |
|---------------------------------------------------------------------------------------|---------------------------------------|
| Cost benefit unknown. | Cost known upfront. |
| You need more granularity than partitioning alone allows. | You need partition-level management.  |
| Your queries commonly use filters or aggregation against multiple particular columns. | Filter or aggregate on single column. |
| The cardinality of the number of values in a column or group of columns is large. | |

### Clustering over paritioning

Like clustering, partitioning uses user-defined partition columns to specify how data is partitioned and what data is
stored in each partition. Unlike clustering, partitioning provides granular query cost estimates before you run a query.

See [Introduction to clustered tables](https://cloud.google.com/bigquery/docs/clustered-tables).

- Partitioning results in a small amount of data per partition (approximately less than 1 GB).
- Partitioning results in a large number of partitions beyond the limits on partitioned tables.
- Partitioning results in your mutation operations modifying the majority of partitions in the table frequently (for
  example, every few minutes).

### Automatic reclustering

As data is added to a clustered table

- The newly inserted data can be written to blocks that contain key ranges that overlap with the key ranges in
  previously written blocks
- These overlapping keys weaken the sort property of the table

To maintain the performance characteristics of a clustered table

- BigQuery performs automatic re-clustering in the background to restore the sort property of the table
- For partitioned tables, clustering is maintained for data within the scope of each partition.

### BigQuery Best Practice

- Cost reduction
  - Avoid `SELECT *`
  - Price your queries before running them
  - Use clustered or partitioned tables
  - Use streaming inserts with caution
  - Materialize query results in stages

In BigQuery, materialized views are precomputed views that periodically cache the results of a query for increased
performance and efficiency. BigQuery leverages precomputed results from materialized views and whenever possible reads
only delta changes from the base tables to compute up-to-date results. See [Introduction to materialized
views](https://cloud.google.com/bigquery/docs/materialized-views-intro)

- Query performance
  - Filter on partitioned columns
  - Denormalizing data
  - Use nested or repeated columns
  - Use external data sources appropriately
  - Don’t use it, in case u want a high query performance
  - Reduce data before using a `JOIN`
  - Do not treat `WITH` clauses as prepared statements
  - Avoid oversharding tables

- Query performance
  - Avoid JavaScript user-defined functions
  - Use approximate aggregation functions (HyperLogLog++)
  - Order Last, for query operations to maximize performance
  - Optimize your join patterns
  - As a best practice, place the table with the largest number of rows first, followed by the table with the fewest
  rows, and then place the remaining tables by decreasing size.


### Internals of BigQuery

**A high-level architecture for BigQuery service**

![p80](images/bq-high-level.png)

See also:

- [BigQuery under the hood](https://cloud.google.com/blog/products/bigquery/bigquery-under-the-hood)
- [A Deep Dive Into Google BigQuery Architecture](https://panoply.io/data-warehouse-guide/bigquery-architecture/)
- [BigQuery explained: An overview of BigQuery’s
  architecture](https://cloud.google.com/blog/products/data-analytics/new-blog-series-bigquery-explained-overview)

**Record-oriented vs column-oriented**

![p81](images/bq-record-vs-column.png)

BigQuery stores table data in columnar format, meaning it stores each column separately. Column-oriented databases are
particularly efficient at scanning individual columns over an entire dataset.

Column-oriented databases are optimized for analytic workloads that aggregate data over a very large number of records.
Often, an analytic query only needs to read a few columns from a table. See [Overview of BigQuery
storage](https://cloud.google.com/bigquery/docs/storage_overview) and [Storage
layout](https://cloud.google.com/bigquery/docs/storage_overview#storage_layout).

**An example of Dremel serving tree**

![p82](images/dremel-serving-tree.png)

### BigQuery References

- [BigQuery How-to-guides](https://cloud.google.com/bigquery/docs/how-to)
- [Dremel: Interactive Analysis of Web-Scale Datasets](https://research.google/pubs/pub36632/)
- [A Deep Dive Into Google BigQuery Architecture](https://panoply.io/data-warehouse-guide/bigquery-architecture/)
- [A Look at Dremel](http://www.goldsborough.me/distributed-systems/2019/05/18/21-09-00-a_look_at_dremel/)

## BigQuery ML

### Overwiew

BigQuery ML lets you create and execute machine learning models in BigQuery using standard SQL queries. BigQuery ML
democratizes machine learning by letting SQL practitioners build models using existing SQL tools and skills. BigQuery ML
increases development speed by eliminating the need to move data.

See [What is BigQuery ML?](https://cloud.google.com/bigquery-ml/docs/introduction#supported_models_in)

- Target audience Data analysts, managers
- No need for Python or Java knowledge
- No need to export data into a different system

- Free
  - 10 GB per month of data storage
  - 1 TB per month of queries processed
  - ML Create model step: First 10 GB per month is free
- [BigQuery ML pricing](https://cloud.google.com/bigquery/pricing#bqml)

![p83](images/bq-ml-pipelines.png)

Supervised (labeled) machine learning model study design overview. Steps for the deployment of a supervised machine
learning model. From left to right, the figure shows the initial team of multidisciplinary experts defining a study
design to address a need. Data are then collected, processed, trained tested, validated, and ultimately deployed. See
Rashidi, Hooman & Tran, Nam & Betts, Elham & Howell, Lydia & Green, Ralph. (2019). [Artificial Intelligence and Machine
Learning in Pathology: The Present Landscape of Supervised
Methods](https://www.researchgate.net/publication/335604816_Artificial_Intelligence_and_Machine_Learning_in_Pathology_The_Present_Landscape_of_Supervised_Methods).

### Model selection guide

**Model selection guide**

![p84](images/bq-model-selection.png)

See [Model selection guide](https://cloud.google.com/bigquery-ml/docs/introduction#model_selection_guide).