# Week 4: Analytics Engineering

This week, we’ll dive into Analytics Engineering.

We’ll cover:

- Basics of analytics engineering
- [dbt Labs](https://www.linkedin.com/company/dbtlabs/) (data build tool)
- Testing and documenting
- Deployment to the cloud and locally
- Visualizing the data with Google Data Studio and [Metabase](https://www.linkedin.com/company/metabase/)

**Goal**: Transforming the data loaded in DWH to Analytical Views developing a [dbt
project](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/taxi_rides_ny/README.md).


# Materials

See
[week_4\_analytics_engineering](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_4_analytics_engineering)
on GitHub and
[slides](https://docs.google.com/presentation/d/1xSll_jv0T8JF4rYZvLHfkJXYqUjPtThA/edit?usp=sharing&ouid=114544032874539580154&rtpof=true&sd=true)

Youtube videos:

- [DE Zoomcamp 4.1.1 - Analytics Engineering Basics](https://www.youtube.com/watch?v=uF76d5EmdtU)
- [DE Zoomcamp 4.1.2 - What is dbt](https://www.youtube.com/watch?v=4eCouvVOJUw)
- [DE Zoomcamp 4.2.1 - Start Your dbt Project: BigQuery and dbt Cloud (Alternative
  A)](https://www.youtube.com/watch?v=iMxh6s_wL4Q)
- [DE Zoomcamp 4.2.2 - Start Your dbt Project: Postgres and dbt Core Locally (Alternative
  B)](https://www.youtube.com/watch?v=1HmL63e-vRs)
- [DE Zoomcamp 4.3.1 - Build the First dbt Models](https://www.youtube.com/watch?v=UVI30Vxzd6c) (✦ see note below)
- [DE Zoomcamp 4.3.2 - Testing and Documenting the Project](https://www.youtube.com/watch?v=UishFmq1hLM) (✦ see note
  below)
- [DE Zoomcamp 4.4.1 - Deployment Using dbt Cloud (Alternative A)](https://www.youtube.com/watch?v=rjf6yZNGX8I)
- [DE Zoomcamp 4.4.2 - Deployment Using dbt Locally (Alternative B)](https://www.youtube.com/watch?v=Cs9Od1pcrzM)
- [DE Zoomcamp 4.5.1 - Visualising the data with Google Data Studio (Alternative
  A)](https://www.youtube.com/watch?v=39nLTs74A3E)
- [DE Zoomcamp 4.5.2 - Visualising the data with Metabase (Alternative B)](https://www.youtube.com/watch?v=BnLkrA7a6gM)

**✦ Note**: These videos are shown entirely on dbt cloud IDE but the same steps can be followed locally on the IDE of
your choice.

## Prerequisites

### Datasets

- A running warehouse (BigQuery or postgres)
- A set of running pipelines ingesting the project dataset (week 3 completed): Taxi Rides NY dataset
  - Yellow taxi data - Years 2019 and 2020
  - Green taxi data - Years 2019 and 2020
  - fhv data - Year 2019.
- Data can be found here: <https://github.com/DataTalksClub/nyc-tlc-data>

### Setting up dbt for using BigQuery (Alternative A - preferred)

You will need to create a dbt cloud account using [this link](https://www.getdbt.com/signup/) and connect to your
warehouse [following these
instructions](https://docs.getdbt.com/docs/dbt-cloud/cloud-configuring-dbt-cloud/cloud-setting-up-bigquery-oauth). More
detailed instructions in
[dbt_cloud_setup.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_4_analytics_engineering/dbt_cloud_setup.md).

### Setting up dbt for using Postgres locally (Alternative B)

As an alternative to the cloud, that require to have a cloud database, you will be able to run the project installing
dbt locally. You can follow the [official dbt documentation](https://docs.getdbt.com/dbt-cli/installation) or use a
docker image from official [dbt repo](https://github.com/dbt-labs/dbt/). You will need to install the latest version
(1.0) with the postgres adapter (dbt-postgres). After local installation you will have to set up the connection to PG in
the `profiles.yml`, you can find the templates
[here](https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile).

## What I did to set my tools

**Note**: For the remainder of these notes, I have chosen to continue (or focus more on) with BigQuery, i.e. alternative
A.

### Setup environment

<div class="formalpara-title">

**File `requirements.txt`**

</div>

``` txt
pandas
pydantic
prefect
prefect-sqlalchemy
prefect-gcp[cloud_storage]
protobuf
pyarrow
pandas-gbq
psycopg2-binary
sqlalchemy
```

Use conda to create a working environment `de-zoomcamp`

``` bash
conda create -n de-zoomcamp python=3.9
conda activate de-zoomcamp
pip install -r requirements.txt
```
