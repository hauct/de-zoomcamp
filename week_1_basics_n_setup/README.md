### Introduction
# Data Engineering Zoomcamp 2023 Week 1: Introduction & Prerequisites
See [README.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/README.md) from week 1.

## Table of Contents
* [Docker + Postgres](#user-content-docker--postgres)
  * [Introduction to Docker](#user-content-introduction-to-docker)
  * Ingesting NY Taxi Data to Postgres
  * Connecting to Postgres with Jupyter and Pandas
  * Connecting pdAgmin and Postgres
  * Dockerizing the Ingestion Script
  * Running Postgres and pgAdmin with Docker-Compose
  * SQL Refresher
  * Port Mapping and Networks in Docker (Bonus)
* GCP + Terraform
  * Introduction to GCP
  * Introduction to Terraform Concepts & GCP Pre-Requisites
  * Workshop: Creating GCP Infrastructure with Terraform
  * Setting up the environment on cloud VM
* See also

## Docker + Postgres
### Introduction to Docker
See [DE Zoomcamp 1.2.1 - Introduction to Docker](https://www.youtube.com/watch?v=EYNwNlOrpr0).

![Docker](images/docker.png).

Download, install and start [Docker Desktop](https://www.docker.com/products/docker-desktop/).

Change to `docker`, this is our working directory now.

Try running a container by running: `$ docker run -d -p 80:80 docker/getting-started`.

Run also this command: `$ docker run hello-world`.

You should see this in your terminal.

``` bash
$ docker run hello-world

Hello from Docker!
This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
    (arm64v8)
 3. The Docker daemon created a new container from that image which runs the
    executable that produces the output you are currently reading.
 4. The Docker daemon streamed that output to the Docker client, which sent it
    to your terminal.

To try something more ambitious, you can run an Ubuntu container with:
 $ docker run -it ubuntu bash

Share images, automate workflows, and more with a free Docker ID:
 https://hub.docker.com/

For more examples and ideas, visit:
 https://docs.docker.com/get-started/
```

Inside **Docker Desktop**, we should see two new containers.

![p01](images/docker_desktop_images.png).

To install Ubuntu, run this command, after that type `ls` to check the directories in the container:
``` bash
$ docker run -it ubuntu
```

![p02](images/ubuntu_bash.png)

To quit Ubuntu, enter `exit`.

To install Python 3.9 et open the Python shell, run the commande `$ docker run -it python:3.9`. But this way of doing
things is discouraged since the Python shell does not allow installing other libraries.

But, a more efficient way to install Python with pandas already installed is with a `Dockerfile`.

<div class="formalpara-title">

**File `Dockerfile`**

</div>

``` txt
FROM python:3.9

RUN pip install pandas

ENTRYPOINT [ "bash" ]
```

Then, in the same directory as the `Dockerfile`, run the following two commands:

``` bash
$ docker build -t test:pandas .
$ docker run -it test:pandas
```

After run both above scripts, you check in your terminal whether pandas is installed.
![p03](images/docker_python_pandas.png)

Now let’s create in the same directory a `pipeline.py` file with the following instructions:

<div class="formalpara-title">

**File `pipeline.py`**

</div>

``` python
import pandas as pd

# some fancy stuff with pandas
print('job finished successfully')
```

Let’s add instructions to the `Dockerfile` file.

<div class="formalpara-title">

**File `Dockerfile`**

</div>

``` txt
FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "bash" ]
```

Then, let’s re-run the same two commands `build` and `run`, and check the result:
![p03](images/docker_python_pipeline.png)

Now let’s add instructions to the `pipeline.py` file.

<div class="formalpara-title">

**File `pipeline.py`**

</div>

``` python
import sys
import pandas as pd

print(sys.argv)
day = sys.argv[1]

print(f'job finished successfully for day = f{day}')
```

Next, let’s modify the `Dockerfile`.

<div class="formalpara-title">

**File `Dockerfile`**

</div>

``` txt
FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY pipeline.py pipeline.py

ENTRYPOINT [ "python", "pipeline.py" ]
```

Then, let’s restart the `build` and the `run`.

``` bash
$ docker build -t test:pandas .
$ docker run -it test:pandas 2021-10-15
['pipeline.py', '2021-10-15']
job finished successfully for day = f2021-10-15
$
```

<div class="note">

**Reminder about Docker CLI**

The `docker ps` command only shows running containers by default. To see all containers, use the `-a` (or `--all`) flag.

See [docker ps](https://docs.docker.com/engine/reference/commandline/ps/).

To stop one or more running containers:

``` bash
$ docker stop [OPTIONS] CONTAINER [CONTAINER...]
```

See [docker stop](https://docs.docker.com/engine/reference/commandline/stop/).

</div>

### Ingesting NY Taxi Data to Postgres
The purpose of this task is to extract data, which is a table of information about trips, from the dataset on the [tlc-trip](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page and then inject the data into the PostgreSQL database

We will use a docker image of postgres. See [postgres](https://hub.docker.com/_/postgres) on DockerHub for more
information.

To start a postgres instance, run this command:

``` bash
$ mkdir ny_taxi_postgres_data
$ docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

You should see in the terminal `database system is ready to accept connections`.

Next, open an other terminal window, and install [pgcli](https://www.pgcli.com/).

If you are using Windows, you can run the `$ pip install pgcli` command.

After install pgcli, open a new terminal, type `pgcli -h localhost -p 5432 -u root -d ny_taxi` to connect to our recent database.The password is `root`.

![p4](images/first_pg_database.png)

Now, install the classic Jupyter Notebook with:

``` bash
pip install notebook
```

See [Install Jupyter](https://jupyter.org/install.html) for more information.

To run the notebook:

``` bash
jupyter notebook
```

Choose the `Python 3 (ipykernel)`.

We will use the data from [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
especially data on :

* <https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet> for Yellow Taxi Trip Records
  (PARQUET) for January 2021.
* <https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv> for Taxi Zone Loopup Table (CSV).
* But it’s better to use data from Google Drive: <https://drive.google.com/uc?id=1p2y79m_q9rPM48cEYwQdrk6XVCTFSyRA>.

So in jupyter add the following statements:

``` python
import pandas as pd
pd.__version__

# Choose one of below
## Parquet method
!wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
!pip install pyarrow
df = pd.read_parquet('yellow_tripdata_2021-01.parquet')

## gz file method
!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
df = pd.read_csv('yellow_tripdata_2021-01.csv.gz', compression='gzip', header=0, sep=',', quotechar='"')

# Google drive method
url_code = '1p2y79m_q9rPM48cEYwQdrk6XVCTFSyRA'
os.system(f'gdown {url_code}')
df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

df.info(verbose=True, show_counts=True)
"""
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1369765 entries, 0 to 1369764
Data columns (total 18 columns):
 #   Column                 Non-Null Count    Dtype
---  ------                 --------------    -----
 0   VendorID               1271413 non-null  float64
 1   tpep_pickup_datetime   1369765 non-null  object
 2   tpep_dropoff_datetime  1369765 non-null  object
 3   passenger_count        1271413 non-null  float64
 4   trip_distance          1369765 non-null  float64
 5   RatecodeID             1271413 non-null  float64
 6   store_and_fwd_flag     1271413 non-null  object
 7   PULocationID           1369765 non-null  int64
 8   DOLocationID           1369765 non-null  int64
 9   payment_type           1271413 non-null  float64
 10  fare_amount            1369765 non-null  float64
 11  extra                  1369765 non-null  float64
 12  mta_tax                1369765 non-null  float64
 13  tip_amount             1369765 non-null  float64
 14  tolls_amount           1369765 non-null  float64
 15  improvement_surcharge  1369765 non-null  float64
 16  total_amount           1369765 non-null  float64
 17  congestion_surcharge   1369765 non-null  float64
dtypes: float64(13), int64(2), object(3)
memory usage: 188.1+ MB
"""
```
The structure of these files is described in le [data-dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

We must convert the "date" fields to "timestamp" in the following way.

``` python
df = pd.read_csv("yellow_head.csv")
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
```

We get the SQL schema like this:

``` python
print(pd.io.sql.get_schema(df, name="yellow_taxi_data"))
"""
CREATE TABLE "yellow_taxi_data" (
"Unnamed: 0" INTEGER,
  "VendorID" REAL,
  "tpep_pickup_datetime" TIMESTAMP,
  "tpep_dropoff_datetime" TIMESTAMP,
  "passenger_count" REAL,
  "trip_distance" REAL,
  "RatecodeID" REAL,
  "store_and_fwd_flag" TEXT,
  "PULocationID" INTEGER,
  "DOLocationID" INTEGER,
  "payment_type" REAL,
  "fare_amount" REAL,
  "extra" REAL,
  "mta_tax" REAL,
  "tip_amount" REAL,
  "tolls_amount" REAL,
  "improvement_surcharge" REAL,
  "total_amount" REAL,
  "congestion_surcharge" REAL
)
"""
```
We will use [SQLALchemy](https://www.sqlalchemy.org/). This tool is normally already installed with anaconda. But if you
don’t have anaconda installed, just run the command `$ pip install sqlalchemy`.

But to load all rather large data, it would be better to partition it.

``` python
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

df = next(df_iter)
len(df)
# 100000
```

To create the table in postgres:

``` python
df.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
```

We check if the table has actually been created in postgres.

``` bash
$ pgcli -h localhost -p 5432 -u root -d ny_taxi
Password for root:
Server: PostgreSQL 13.9 (Debian 13.9-1.pgdg110+1)
Version: 3.5.0
Home: http://pgcli.com
root@localhost:ny_taxi> \dt
+--------+------------------+-------+-------+
| Schema | Name             | Type  | Owner |
|--------+------------------+-------+-------|
| public | yellow_taxi_data | table | root  |
+--------+------------------+-------+-------+
SELECT 1
Time: 0.032s
root@localhost:ny_taxi> \d yellow_taxi_data

+-----------------------+-----------------------------+-----------+
| Column                | Type                        | Modifiers |
|-----------------------+-----------------------------+-----------|
| index                 | bigint                      |           |
| VendorID              | bigint                      |           |
| tpep_pickup_datetime  | timestamp without time zone |           |
| tpep_dropoff_datetime | timestamp without time zone |           |
| passenger_count       | double precision            |           |
| trip_distance         | double precision            |           |
| RatecodeID            | double precision            |           |
| store_and_fwd_flag    | text                        |           |
| PULocationID          | bigint                      |           |
| DOLocationID          | bigint                      |           |
| payment_type          | bigint                      |           |
| fare_amount           | double precision            |           |
| extra                 | double precision            |           |
| mta_tax               | double precision            |           |
| tip_amount            | double precision            |           |
| tolls_amount          | double precision            |           |
| improvement_surcharge | double precision            |           |
| total_amount          | double precision            |           |
| congestion_surcharge  | double precision            |           |
| airport_fee           | double precision            |           |
+-----------------------+-----------------------------+-----------+
Indexes:
    "ix_yellow_taxi_data_index" btree (index)
```

To load all data, do this:

``` python
%time df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
# CPU times: user 3.22 s, sys: 82.7 ms, total: 3.3 s
# Wall time: 8.38 s
#1000
```

Pour vérifier s’il existe la table **yellow_taxi_data** contient toutes les données, faire ceci:

``` bash
root@localhost:ny_taxi> SELECT count(1) FROM yellow_taxi_data;
+--------+
| count  |
|--------|
| 100000 |
+--------+
SELECT 1
Time: 0.045s
root@localhost:ny_taxi>
```
To load all chunk data into the database, do this:

``` python
from time import time

while True:
    t_start = time()
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    t_end = time()

    print('insert another chunk..., took %.3f second' % (t_end - t_start))
"""
inserted another chunk, took 16.199 second
inserted another chunk, took 16.468 second
inserted another chunk, took 15.576 second
inserted another chunk, took 16.555 second
inserted another chunk, took 16.935 second
inserted another chunk, took 16.828 second
inserted another chunk, took 15.195 second
inserted another chunk, took 16.082 second
inserted another chunk, took 15.837 second
inserted another chunk, took 15.499 second
inserted another chunk, took 15.782 second
C:\Users\LAP14062-local\AppData\Local\Temp\ipykernel_10200\2113580993.py:4: DtypeWarning: Columns (6) have mixed types. Specify dtype option on import or set low_memory=False.
  df = next(df_iter)
inserted another chunk, took 16.097 second
inserted another chunk, took 9.861 second
"""
```
It seems that I have an error.

Let’s check the number of rows in the postgres database.

``` bash
root@localhost:ny_taxi> SELECT count(1) FROM yellow_taxi_data;
+---------+
| count   |
|---------|
| 1369765 |
+---------+
SELECT 1
Time: 0.472s
root@localhost:ny_taxi>
```

I have 1369765 rows in the database while the source file contains 1369766. So I am missing one row.

For more details, you can check the code `upload-data.ipynb` on folder `2_docker_sql`
[pgAdmin](https://www.pgadmin.org/) is easier to use than **pgcli**.

Since we already have Docker, we don’t need to install pgAdmin. We only need to use a docker image from pgAdmin.

Go to [pgAdmin 4 (Container)](https://www.pgadmin.org/download/pgadmin-4-container/), or directly to
[pgadmin4](https://www.pgadmin.org/download/pgadmin-4-container/) on DockerHub.

Here is the command to run pgAdmin in Docker.

``` bash
$ docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

Then open the browser to <http://localhost:8080/> and you should see this.
![p05](images/pgadmin_1.png)

Enter username `admin@admin.com` and password `root`, and you should see this.
![p06](images/pgadmin_2.png)


We must then create a server. Click on **Add New Server** and identify the postgres instance located in another
container.

But this will not work since we must ensure that the two containers can communicate with each other. To do this, we will
use **docker network**.

Stop both containers (`Postgres` and `pgAdmin`) and go to [docker network create](https://docs.docker.com/engine/reference/commandline/network_create/).

In a new terminal window, run this command to create a new network:

``` bash
$ docker network create pg-network
$ docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

With pgcli, we can check if our connection is still working and that the data is still present.

``` txt
root@localhost:ny_taxi> SELECT COUNT(1) FROM yellow_taxi_data;
Reconnecting...
Reconnected!
+---------+
| count   |
|---------|
| 1369765 |
+---------+
SELECT 1
Time: 0.305s
root@localhost:ny_taxi>
```

Then run the following command in another terminal window.

``` bash
$ docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin-2 \
  dpage/pgadmin4
```

Then go to <http://localhost:8080/> again, enter username `admin@admin.com` and password `root`.

Then create a server. Click on **Add New Server**, enter the information as below (still with username `admin@admin.com`
and password `root`), then click the **Save** button.

|                            |                            |
|----------------------------|----------------------------|
| ![p07](images/sever_1.png) | ![p08](images/sever_2.png) |

In the left menu, click successively on **Server**, **Local Docker**, **Database**, **ny_taxi**, **Schemas**,
**public**, **Tables**, **yellow_taxi_data**. After, right-click and select **View/Edit Data** and **First 100 Rows**.

We should see this after asking to display the first 100 rows of the **yellow_taxi_data** table.

![p09](images/query_1.png)

Enter this query `SELECT COUNT(1) FROM yellow_daxi_data;`.

![p10](images/query_2.png)

### Dockerizing the Ingestion Script
See [DE Zoomcamp 1.2.4 - Dockerizing the Ingestion Script](https://www.youtube.com/watch?v=B1WwATwf-vY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=8) on Youtube.

Please check the file `upload-data.py` on folder `2_docker_sql`, you will see this full script:

Note that we will use [argparse](https://docs.python.org/3/library/argparse.html).

<div class="formalpara-title">

**File `upload-data.py`**

</div>

``` python

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
```

Before run this script, we need to drop **yellow_taxi_data** table with pgAdmin with this SQL:

``` sql
DROP TABLE yellow_taxi_data;
SELECT COUNT(1) FROM yellow_taxi_data;
-- ERROR:  relation "yellow_taxi_data" does not exist
-- LINE 2: SELECT COUNT(1) FROM yellow_taxi_data;
```

Now, we could run this script `upload-data.py`:

``` bash
$ python3 upload-data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
```
When the execution is finished, one can check in pgAdmin if the table is complete.

``` sql
SELECT COUNT(1) FROM yellow_taxi_trips;
-- 1369765
```

Now, we will dockerize this ingestion script.

We need to destroy the table in Postgres and adjust the `Dockerfile`.

``` sql
DROP TABLE yellow_taxi_trips;
-- Query returned successfully in 117 msec.
```

<div class="formalpara-title">

**File `Dockerfile`**

</div>

``` txt
FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY upload-data.py upload-data.py

ENTRYPOINT [ "python", "upload-data.py" ]
```

Then, we execute the following command:

``` bash
$ docker build -t taxi_ingest:v001 .
```

Then, we execute the following command:

``` bash
$ docker run -it \
    --network=pg-network \ 
    taxi_ingest:v001 \
      --user=root \
      --password=root \
      --host=pg-database \ 
      --port=5432 \
      --db=ny_taxi \
      --table_name=yellow_taxi_trips \
```

- We should run this thing in the network, and not on the localhost.

- Who need also to use `pd-database`.

Let check again if the data is export to Postgre database

``` sql
SELECT COUNT(1) FROM yellow_taxi_trips;
-- 1369765
```

<div class="note">

Remember that we list the containers with the command `docker ps` and stop containers with the command
`docker kill <CONTAINER_ID>`.

</div>

<div class="note">

**15:15 HTTP server + ipconfig**  
We also mention the possibility of creating your own http server with the commands `python3 -m http.server` and
`ifconfig` (`ipconfig` on windows) then change the URL to search for the source file on our own machine.

</div>

### Running Postgres and pgAdmin with Docker-Compose
See [DE Zoomcamp 1.2.5 - Running Postgres and pgAdmin with Docker-Compose](https://www.youtube.com/watch?v=hKI6PkPhpa0)
on Youtube.

#### Introduction to Docker-Compose
So far we have run several commands to create docker instances, load data, etc. We’re gonna make it all simple using
[docker compose](https://docs.docker.com/compose/).

Compose is a tool for defining and running multi-container Docker applications. With Compose, you use a YAML file to
configure your application’s services. Then, with a single command, you create and start all the services from your
configuration.

#### Installing Docker-Compose
Normally, Docker compose is already installed since it is included in Docker Desktop.

#### Configuration of postgres database and pgadmin in Docker-Compose file
With Docker compose, the images will be installed automatically in the same network.

See
[docker-compose.yaml](2_docker_sql/docker-compose.yaml).

<div class="formalpara-title">

**File `docker-compose.yaml`**

</div>

``` yaml
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
```
For more details, please check file `docker-compose.yaml` on folder `2_docker_sql`

#### Running the Docker-Compose file
We need to stop the current running containers **pgadmin** et **postgres**.

![p11](images/docker_stop.png)

Then run this command:

``` bash
$ docker-compose up
```

Then go to <http://localhost:8080/> again, enter username `admin@admin.com` and password `root`.

Then create a server. Click **Add New Server**, enter the following information, then click the **Save** button.

Tab **General**:

* Name: `Local Docker`

Tab **Connection**:

* Host name: `pgdatabase`
* Port: `5432`
* Username: `root`
* Password: `root`

Let’s check if the database table is correct.

![p12](images/sever_3.png)


#### Stopping the running containers with Docker-Compose
To stop this running docker compose, just run the command `$ docker-compose down`.

#### Running Docker-Compose in detached mode
You can restart in detached mode, with the command `$ docker-compose up -d`. This way allows us to find the terminal
window and you don’t need to open a new window.

### Port Mapping and Networks in Docker (Bonus)
See [DE Zoomcamp 1.4.2 - Port Mapping and Networks in Docker (Bonus)](https://www.youtube.com/watch?v=tOr4hTsHOzU).

* Docker networks
* Port forwarding to the host environment
* Communicating between containers in the network
* `.dockerignore` file

![p13](images/port_mapping.png)

## GCP + Terraform
The code is [here](/1_terraform_gcp/)

### Introduction to Google Cloud Platform
See [DE Zoomcamp 1.1.1 - Introduction to Google Cloud Platform](https://www.youtube.com/watch?v=18jIzE41fJ4) on Youtube.

### Introduction to Terraform Concepts & GCP Pre-Requisites
See [DE Zoomcamp 1.3.1 - Introduction to Terraform Concepts & GCP Pre-Requisites](https://www.youtube.com/watch?v=Hajwnmj0xfQ) on Youtube and [1_terraform_overview.md](1_terraform_gcp/1_terraform_overview.md) on GitHub.

* What is Terraform?
  * [Terraform](https://www.terraform.io/) is a tool for build, change, and destroy infrastructure.
  * Open-source tool by HashiCorp, used for provisioning infrastructure resources.
  * Supports DevOps best practices for change management.
  * Managing configuration files in source control to maintain an ideal provisioning state for testing and production
    environments.

* What is IaC?
  * Infrastructure-as-Code.
  * Build, change, and manage your infrastructure in a safe, consistent, and repeatable way by defining resource
    configurations that you can version, reuse, and share.

* Some advantages
  * Infrastructure lifecycle management.
  * Version control commits.
  * Very useful for stack-based deployments, and with cloud providers such as AWS, GCP, Azure, K8S.
  * State-based approach to track resource changes throughout deployments.

See [Install Terraform](https://developer.hashicorp.com/terraform/downloads) and [Install Terraform
CLI](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/install-cli).


I opened my free GCP account with \$300 credit to spend within 90 days.

I was asked for my credit card. But it was clearly indicated :  
**No autocharge after free trial ends**  
We ask you for your credit card to make sure you are not a robot. You won’t be charged unless you manually upgrade to a
paid account.

Go to the [Google Cloud Console](https://console.cloud.google.com/welcome).

I created the project ny-rides-hauct (for DataTalks.Club Data Engineering Zoomcamp).

|                                   |                                    |
|-----------------------------------|------------------------------------|
|![p14](images/create_project_1.png)|![p15](images/create_project_2.png) |

Select the ny-rides-hauct project and go to the left menu `IAM & Admin`, then `Service Accounts`.

![p16](images/service_account_1.png)

We must then click on **+ CREATE SERVICE ACCOUNT** located at the top.

See [Understanding service accounts"](https://cloud.google.com/iam/docs/understanding-service-accounts).

Enter **ny-rides-viewr** in the **Service account name** field, then click on **CREATE AND CONTINUE** button.

Select `Viewer` in the **Role** field, then click the **DONE** button.

Right under **Actions**, select **Manage keys**. Then under the **ADD KEY** button, select **Create New Key** and keep
**JSON** as key type.

Save the private key `ny-rides-hauct-397604-5b0b890dd98c.json` to your computer (like here `C:/Users/cungt/.gc`).

Download [Google Cloud CLI](https://cloud.google.com/sdk/docs/install-sdk) for local setup.

I downloaded the [file](https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe) for Windows.
Open the file and keep default settings, the installation will run

The main folder is `C:/Users/cungt/AppData/Local/Google/Cloud SDK`

Open your terminal and type
``` bash
gcloud -v
```
![p17](images/gcp_local_check.png)

Then I run this:

``` bash
$ gcloud components update
# Do you want to continue (Y/n)? Y
```

See [Install the Google Cloud CLI](https://cloud.google.com/sdk/docs/install-sdk) for more information.

Finally, I ran the following command:
``` bash
# Refresh token/session, and verify authentication
$ export GOOGLE_APPLICATION_CREDENTIALS="C:/Users/cungt/.gc/ny-rides-hauct-397604-5b0b890dd98c.json"
$ gcloud auth application-default login
```

Select the required account, click on **Allow** button and then Google tells me that **You are now authenticated with
the gcloud CLI!**.

I already installed Terraform. If not, go to [Install Terraform](https://developer.hashicorp.com/terraform/downloads).

We will use Google Cloud Storage (GCS) for Data Lake and BigQuery for Data Warehouse.

In the GCP Console, go to the left menu **IAM**, then click on the pencil to edit permissions of the key that has just
been created. We must add the roles, **Storage Object Admin** and **BigQuery Admin**, then click on
the **Save** button.


![p18](images/edit_role.png)

Go to [IAM Service Account Credentials
API](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com), make sure to select the right project
**ny-rides-hauct** and click **ENABLE** button.

### Workshop: Creating GCP Infrastructure with Terraform
> 2023-01-21.

See [DE Zoomcamp 1.3.2 - Creating GCP Infrastructure with
Terraform](https://www.youtube.com/watch?v=dNkEgO-CExg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12).

See also on GitHub repository :

* [1_terraform_overview.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_1_basics_n_setup/1_terraform_gcp/1_terraform_overview.md)
  on GitHub.
* [Wirkshop](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_1_basics_n_setup/1_terraform_gcp/terraform)

Important files to use Terraform are `main.tf` and `variables.tf`. I can have an option the `resources.tf` and
`output.tf` files.

For this section, create a directory and copy these files inside.
``` bash
$ mkdir ~/learning/terraform
$ cp main.tf variables.tf ~/learning/terraform
```

We must specify in `main.tf` the resources we want to use.

Note that I installed the [Terraform extension for Visual Studio
Code](https://marketplace.visualstudio.com/items?itemName=HashiCorp.terraform). See [Terraform Learning
Guides](https://developer.hashicorp.com/terraform/tutorials).

The `main.tf` file contains the instructions to create a new bucket in Google cloud storage service (GCS). See
[google_storage_bucket](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket)
for a predefined definition specific for GCS.

See also [Google Cloud Storage documentation](https://cloud.google.com/storage/docs).

<div class="formalpara-title">

**File `main.tf`**

</div>

``` txt
terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project
  region = var.region
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project    = var.project
  location   = var.region
}
```

The file `variables.tf` contains configuration. In particular, we must choose the region from
<https://cloud.google.com/about/locations>. In my case, I choose Montréal `asia-southeast1-b`.

<div class="formalpara-title">

**File `variables.tf`**

</div>

``` txt
locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-southeast1-b"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}
```
Before running terraform, we must know the execution steps:

1. `terraform init`: Initializes and configures the backend, installs plugins/providers, and checks out an existing configuration from a version control.
2. `terraform plan`: Matches/previews local changes against a remote state, and proposes an Execution Plan.
3. `terraform apply`: Asks for approval to the proposed plan, and applies changes to cloud.
4. `terraform destroy`: Removes your stack from the Cloud.

Run the following commands:

``` bash
# Refresh service-account's auth-token for this session
$ export GOOGLE_APPLICATION_CREDENTIALS="C:/Users/cungt/.gc/ny-rides-hauct-397604-5b0b890dd98c.json"
$ gcloud auth application-default login
# Select your account and enter your password.
# You should see "You are now authenticated with the gcloud CLI!"
```

Next, rather than indicating the project id in the `variables.tf` file, we will pass the Project ID at runtime. My
project ID is **ny-rides-hauct-397604**.

Execute the following commands.

``` bash
# Initialize state file (.tfstate)
$ terraform init
# Terraform has been successfully initialized!
```
![p19](images/terraform_init.png)

```bash
# Check changes to new infra plan
$ terraform plan
# Enter the project-id: ny-rides-hauct-397604
```
![p20](images/terraform_plan.png)

Note that we could have passed the project id this way.

``` bash
# Check changes to new infra plan
terraform plan -var="project=<your-gcp-project-id>"
# Create new infra
terraform apply -var="project=<your-gcp-project-id>"
```
Terraform tells that it will performa the following actions:

* **google_bigquery_dataset.dataset** will be created
* **google_storage_bucket.data-lake-bucket** will be created
* Plan: 2 to add, 0 to change, 0 to destroy.

Now, execute the following commands.

``` bash
$ terraform apply
```
### Setting up the environment on cloud VM
> 2023-01-22.

See [DE Zoomcamp 1.4.1 - Setting up the Environment on Google Cloud (Cloud VM + SSH
access)](https://www.youtube.com/watch?v=ae-CV2KfoN0).

Go to **Google Cloud**, in the left menu, select **Compute Engine**, **VM instances**, make sure you select the right
project (**ny-rides-alexey-396910**), and click on the **ENABLE** button if not already done.

![p20](images/vm_instance_alexey.png)

We must then generate the SSH key that we will use for this instance. For more information, see:

- [Providing public SSH keys to
  instanc](https://cloud.google.com/compute/docs/instances/connecting-advanced#provide-key)

- [Create SSH keys](https://cloud.google.com/compute/docs/connect/create-ssh-keys)

- [Add SSH keys to VMs](https://cloud.google.com/compute/docs/connect/add-ssh-keys)

- [Run Docker commands without sudo](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md)

But, first let create a folder that save all your personal key

```bash
$ mkdir .ssh
```
#### Generate ssh keys
then run this to generate key

```bash
$ cd ~/.ssh
$ ssh-keygen -t rsa -f gcp -C hauct -b 2048
$ cat gcp.pub
```
#### Upload public key to GCP
We must then provide the public key `gcp.pub` to Google Cloud. To do this, go to **Compute Engine**, **Metadata**, **SSH
KEYS** tab, and click the **ADD SSH KEY** button.

![p21](images/ssh_key.png)

#### Create VM
Then, let’s go back to **Google Cloud**, **Compute Engine**, **VM instances** then click on **CREATE INSTANCE** with the
following choices.

|                                     |                                     |
|-------------------------------------|-------------------------------------|
| ![p22](images/create_instance1.png) | ![p23](images/create_instance2.png) |

We get this.

![p24](images/create_instance3.png)

#### ssh into VM
Copy the External IP (34.142.227.31) of the instance you've created
![p25](images/external_ip.png)

go to the terminal and run the following command:

```bash
cungt@LAPTOP-2KBDAOLD MINGW64 ~
$ ssh -i ~/.ssh/gcp hauct@34.142.227.31
<<comment
The authenticity of host '34.142.227.31 (34.142.227.31)' can't be established.
ED25519 key fingerprint is SHA256:WeC9Dmo8xWwK3tdr/IT/n2/tAbijRbYeG0lwaj/0Skk.
This host key is known by the following other names/addresses:
    ~/.ssh/known_hosts:4: 34.87.139.49
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added '34.142.227.31' (ED25519) to the list of known hosts.
Welcome to Ubuntu 20.04.6 LTS (GNU/Linux 5.15.0-1039-gcp x86_64)

 * Documentation:  https://help.ubuntu.com
 * Management:     https://landscape.canonical.com
 * Support:        https://ubuntu.com/advantage

  System information as of Fri Sep  1 12:27:19 UTC 2023

  System load:  0.0                Users logged in:                  0
  Usage of /:   44.6% of 28.89GB   IPv4 address for br-64f7bafc5050: 172.18.0.1
  Memory usage: 2%                 IPv4 address for docker0:         172.17.0.1
  Swap usage:   0%                 IPv4 address for ens4:            10.148.0.2
  Processes:    121

 * Strictly confined Kubernetes makes edge and IoT secure. Learn how MicroK8s
   just raised the bar for easy, resilient and secure K8s cluster deployment.

   https://ubuntu.com/engage/secure-kubernetes-at-the-edge

Expanded Security Maintenance for Applications is not enabled.

2 updates can be applied immediately.
To see these additional updates run: apt list --upgradable

Enable ESM Apps to receive additional future security updates.
See https://ubuntu.com/esm or run: sudo pro status


*** System restart required ***
Last login: Wed Aug 30 09:02:51 2023 from 1.53.255.147
comment
(base) hauct@de-zoomcamp:~$
```
``` bash
# This command allows to have information on the machine...
(base) hauct@de-zoomcamp:~$ gcloud --version
<<comment
Google Cloud SDK 444.0.0
alpha 2023.08.22
beta 2023.08.22
bq 2.0.97
bundled-python3-unix 3.9.16
core 2023.08.22
gcloud-crc32c 1.0.0
gsutil 5.25
minikube 1.31.2
skaffold 2.6.0
comment
(base) hauct@de-zoomcamp:~$
```
Download and install **Anaconda for Linux** in our instance. See
[Downloads](https://www.anaconda.com/products/distribution#Downloads) and
<https://repo.anaconda.com/archive/Anaconda3-2023.07-2-Linux-x86_64.sh>.

``` bash
hauct@de-zoomcamp:~$ wget https://repo.anaconda.com/archive/Anaconda3-2023.07-2-Linux-x86_64.sh
hauct@de-zoomcamp:~$ bash Anaconda3-2023.07-2-Linux-x86_64.sh
# Read and accept license.
hauct@de-zoomcamp:~$ ls
# Anaconda3-2023.07-2-Linux-x86_64.sh  anaconda3  snap
```

Now we logout the sever

```bash
hauct@de-zoomcamp:~$ logout
```

#### Configure VM and setup local \C:/Users/cungt/.ssh/config
Next, create the `C:/Users/cungt/.ssh/config` file on my Windows with this information:

<div class="formalpara-title">

**Fichier `C:/Users/cungt/.ssh/config`**

</div>

``` txt
Host de-zoomcamp:
    HostName 34.142.227.31
    User hauct
    IdentityFile C:/Users/cungt/.ssh/gcp
    LocalForward 5432 localhost:5432
```

Then, we only have this command to do to enter the server:

``` bash
$ ssh de-zoomcamp
```
In Linux server, we can do this command to know where is python.

``` bash
(base) hauct@de-zoomcamp:~$ which python
/home/hauct/anaconda3/bin/python
(base) hauct@de-zoomcamp:~$
```

Now let’s install Docker.

``` bash
hauct@de-zoomcamp:~$ sudo apt-get update
hauct@de-zoomcamp:~$ sudo apt-get install docker.io
```

#### ssh with VS Code
In VS Code, find and install the **Remote - SSH** extension. Then go to the **Command Palette** (kbd:\[Shift+Cmd+P\])
and select **Remote-SSH: Connect to Host…​** and **de-zoomcamp**.

![p26](images/vs_connect_host1.png)

We now have VS Code pointing to the server.

![p27](images/vs_connect_host2.png)

We need to make a clone of the github on the server.

``` bash
(base) hauct@de-zoomcamp:~$ git clone https://github.com/hauct/de-zoomcamp.git
<<comment
Cloning into 'de-zoomcamp'...
remote: Enumerating objects: 2520, done.
remote: Counting objects: 100% (221/221), done.
remote: Compressing objects: 100% (140/140), done.
remote: Total 2520 (delta 83), reused 170 (delta 48), pack-reused 2299
Receiving objects: 100% (2520/2520), 1.29 MiB | 17.17 MiB/s, done.
Resolving deltas: 100% (1373/1373), done.
comment
(base) hauct@de-zoomcamp:~$
```

Next, we need to authorize docker. Go on at [Run Docker commands without
sudo](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md) for instructions.

On the server, run the following commands:

``` bash
(base) hauct@de-zoomcamp:~$ sudo groupadd docker
(base) hauct@de-zoomcamp:~$ sudo gpasswd -a $USER docker
(base) hauct@de-zoomcamp:~$ sudo service docker restart
(base) hauct@de-zoomcamp:~$ logout
$ ssh de-zoomcamp
(base) hauct@de-zoomcamp:~$ docker run hello-world
(base) hauct@de-zoomcamp:~$ docker run -it ubuntu bash
<<comment
Unable to find image 'ubuntu:latest' locally
latest: Pulling from library/ubuntu
6e3729cf69e0: Pull complete
Digest: sha256:27cb6e6ccef575a4698b66f5de06c7ecd61589132d5a91d098f7f3f9285415a9
Status: Downloaded newer image for ubuntu:latest
comment
root@e2ee01058851:/# ls
bin   dev  home  lib32  libx32  mnt  proc  run   srv  tmp  var
boot  etc  lib   lib64  media   opt  root  sbin  sys  usr
root@e2ee01058851:/# exit
# exit
(base) hauct@de-zoomcamp:~$
```
Now let’s install Docker Compose. Go to <https://github.com/docker/compose>, select the **latest releases**, and
**docker-compose-linux-x86_64**, copy the link.

In the server, do the following commands:

``` bash
(base) hauct@de-zoomcamp:~$ mkdir bin
(base) hauct@de-zoomcamp:~$ cd bin
(base) hauct@de-zoomcamp:~/bin$ wget https://github.com/docker/compose/releases/download/v2.15.0/docker-compose-linux-x86_64 -O docker-compose
(base) hauct@de-zoomcamp:~/bin$ ls
# docker-compose
(base) hauct@de-zoomcamp:~/bin$ chmod +x docker-compose
(base) hauct@de-zoomcamp:~/bin$ ./docker-compose version
# Docker Compose version v2.15.0
(base) hauct@de-zoomcamp:~/bin$ cd
(base) hauct@de-zoomcamp:~$ nano .bashrc
```

We need to add the `/bin` directory to the `PATH` by addind this instruction at the bottom of the `.bashrc` file:

``` bash
export PATH="${HOME}/bin:${PATH}"
```

![p28](images/nano_bashrc.png)

Do kbd:\[Ctrl+O\] and kbd:\[Enter\] to save and kbd:\[Ctrl+X\] to quit. Then issue the `source .bashrc` command.
This will help re-update `.bashrc` file

Now we will run `docker-compose` to create a container that have `PostgresSQL` and `pgAdmin` on this server (VM)

Change to the working directory `~/de-zoomcamp/week_1_basics_n_setup/2_docker_sql`
and run this following scripts

``` bash
(base) hauct@de-zoomcamp:~$ cd ~/de-zoomcamp/week_1_basics_n_setup/2_docker_sql
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/2_docker_sql$ ls
# Dockerfile  docker-compose.yaml    pg-test-connection.ipynb  upload-data.ipynb  yellow_tripdata_2021-01.csv
# README.md   my_taxi_postgres_data  scripts.txt               upload-data.py
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/2_docker_sql$ docker-compose up -d
# [+] Running 2/2
 # ✔ Container 2_docker_sql-pgadmin-1     Started                                                                                                    0.0s 
 # ✔ Container 2_docker_sql-pgdatabase-1  Started                                                                                                    0.0s 
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/2_docker_sql$ 
```

To check running container:
```bash
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/2_docker_sql$ docker ps
# CONTAINER ID   IMAGE            COMMAND                  CREATED      STATUS              PORTS                                            NAMES
# 92bf80a9e249   dpage/pgadmin4   "/entrypoint.sh"         4 days ago   Up About a minute   443/tcp, 0.0.0.0:8080->80/tcp, :::8080->80/tcp   2_docker_sql-pgadmin-1
# 45671e6e71bf   postgres:13      "docker-entrypoint.s…"   4 days ago   Up About a minute   0.0.0.0:5432->5432/tcp, :::5432->5432/tcp        2_docker_sql-pgdatabase-1
```

#### Install pgcli (use conda)
We must now install **pgcli**. I will user the method **conda**

```bash
(base) hauct@de-zoomcamp:~$ conda install -c conda-forge pgcli
(base) hauct@de-zoomcamp:~$ pip install -U mycli
```

#### Setup port forwarding to local machine
The instruction says to open a **PORT 5432** in the server’s VS Code. In your VS code, if you haven't fowarded server port to you local port, follow this instruction to forward a port

|                                 |                                 |
|---------------------------------|---------------------------------|
| ![p29](images/foward_port1.png) | ![p30](images/foward_port2.png) |

Now, I can now use pgcli on the client side (i.e. on my Windows) to access the database located on the server.

![p30](images/vs_connected.png)

We also open the **PORT 8080** (pgAdmin).
Image `pgAdmin` has been running on our container, so we just need to forward on VS Code.


This is the completely ports setting

![p32](images/vs_complete_ports.png)

In VS Code of the server, in order to check the data later ,I open **PORT 8080** and I open <http://localhost:8080> on my Windows and pgAdmin appears.
Enter the username `admin@admin.com` and the password `root`.

#### Run Jupyter to run upload-data notebook
We need to open the **PORT 8888** (Jupyter notebook) in the server's VS Code.

Before you foward `Jupyter notebook` port, run this scrip on server's terminal



``` bash
(base) hauct@de-zoomcamp:~$ ~/de-zoomcamp/week_1_basics_n_setup/2_docker_sql
(base) hauct@de-zoomcamp:~$ jupyter notebook
```

![p31](images/sever_jn.png)

Now, I can start jupyter on the server side. Open <http://localhost:8888//?token> and open `upload-data.ipynb`.

On the server, I can download the following files.

``` bash
(base) hauct@de-zoomcamp:~$ wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
```

**Note**: In file `upload-data.ipynb`, i added the code downloading the data above.

We can now check if we can load data into a postgres table with this python code in jupyter.

``` python
import pandas as pd

!pip install pyarrow
!pip install psycopg2-binary

df = pd.read_csv('yellow_tripdata_2021-01.csv.gz', nrows=100)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
```

Check if the table exists on the server.

``` bash
$ pgcli -h localhost -U root -d ny_taxi
Server: PostgreSQL 13.12 (Debian 13.12-1.pgdg120+1)
Version: 3.5.0
Home: http://pgcli.com
root@localhost:ny_taxi> \dt
+--------+------------------+-------+-------+
| Schema | Name             | Type  | Owner |
|--------+------------------+-------+-------|
| public | yellow_taxi_data | table | root  |
+--------+------------------+-------+-------+
SELECT 1
Time: 0.067s
```
#### Install Terraform
We now need to install Terraform. One should get the zip file link from https://developer.hashicorp.com/terraform/downloads. We must take AMD64.

``` bash
(base) hauct@de-zoomcamp:~$ cd bin
(base) hauct@de-zoomcamp:~$ ls
# docker-compose
(base) hauct@de-zoomcamp:~/bin$ wget https://releases.hashicorp.com/terraform/1.3.7/terraform_1.5.6_linux_amd64.zip
(base) hauct@de-zoomcamp:~/bin$ ls
# docker-compose  terraform_1.5.6_linux_amd64.zip
(base) hauct@de-zoomcamp:~/bin$ sudo apt-get install unzip
(base) hauct@de-zoomcamp:~/bin$ unzip terraform_1.5.6_linux_amd64.zip
# Archive:  terraform_1.5.6_linux_amd64.zip
#   inflating: terraform
(base) hauct@de-zoomcamp:~/bin$ rm terraform_1.5.6_linux_amd64.zip
(base) hauct@de-zoomcamp:~/bin$ ls
docker-compose  terraform  # terraform is green so it's an executable.
(base) hauct@de-zoomcamp:~/bin$ terraform -version
Terraform v1.5.6
on linux_amd64
(base) hauct@de-zoomcamp:~/bin$
```

#### sftp Google credentials to VM
Now I want to upload my `C:/Users/cungt/.gc/ny-rides-hauct-397604-5b0b890dd98c.json` file to the server. To do
this, run the following command from the terminal on my Windows

``` bash
scp -i ~/.ssh/gcp ~/.gc/ny-rides-hauct-397604-5b0b890dd98c.json hauct@34.142.227.31:~
```

**Note**: This script has the format:
``` bash
scp -i [path_to_private_key] [local_file_path] [user]@[vm_external_ip]:[remote_file_path]
```
and then VS Code of the server, run this script to move the file to `.gc` folder:

``` bash
# Create `.gc` folder
(base) hauct@de-zoomcamp:~$ mkdir .gc
(base) hauct@de-zoomcamp:~$ mv ny-rides-hauct-397604-5b0b890dd98c.json .gc/
```

#### Configure gcloud
Run these commands on the server.

``` bash
(base) hauct@de-zoomcamp:~$ cd ~/de-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform$ export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/ny-rides-hauct-397604-5b0b890dd98c.json
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform$ gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
# Activated service account credentials for: [ny-rides-viewer@ny-rides-hauct-397604.iam.gserviceaccount.com]
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform$ 
```

#### Run Terraform commands

On the server, I need to change the region in the `variable.tf` file in the `terraform` directory. I can also define a
variable containing my project id.

Now, run these scripts:
```bash
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform$ terraform init
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform$ terraform plan
(base) hauct@de-zoomcamp:~/de-zoomcamp/week_1_basics_n_setup/1_terraform_gcp/terraform$ terraform apply
```
#### Shut down VM

To stop the server, do this command:

``` bash
(base) hauct@de-zoomcamp:~$ sudo shutdown
# Shutdown scheduled for Fri 2023-09-01 16:55:35 UTC, use 'shutdown -c' to cancel.
(base) hauct@de-zoomcamp:~$
```

It is also recommended to **STOP** on the instance in the Google Cloud interface.

Go to **Google Cloud**, in the left menu, select **Compute Engine**, **VM instances**, make sure you select the right project (**ny-rides-alexey**), and click on **STOP**.

![p32](images/vm_instance_shutdown.png)

#### Start VM back up and update \~/.ssh/config

You can restart the instance by clicking on **START** of the instance in the Google Cloud interface.

Usually, you will get a new **External ID**, which require to re-edit in the `~/.ssh/config` file.

Oddly, I get the same IP address, so I don’t need to change the config file.

<div class="formalpara-title">

**Fichier `~/.ssh/config`**

</div>

``` bash
Host de-zoomcamp:
    HostName 34.142.227.31
    User hauct
    IdentityFile C:/Users/cungt/.ssh/gcp
    LocalForward 5432 localhost:5432
```

Then I can access the server the same way.

``` bash
$ ssh de-zoomcamp
```

And we find our instance with all the software installed.

#### Delete VM

The instructor destroys the instance by clicking on **DELETE** next to the instance.

#### Explanation of GCP charges

The instructor explains to us that we pay for the CPU and for the storage used.

## See also

* [Fixing TLC Yellow Taxi 2019 Data Parquet Errors Loading Into Big Query](https://www.youtube.com/watch?v=wkgDUsDZKfg)
* [SSH Simplified: Aliasing Credentials with Config Files](https://itsadityagupta.hashnode.dev/ssh-simplified-aliasing-credentials-with-config-files)
