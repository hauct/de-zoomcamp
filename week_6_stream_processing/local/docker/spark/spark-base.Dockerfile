# Use the "cluster-base" image as the base image
FROM cluster-base

# -- Layer: Apache Spark

# Define arguments for Spark and Hadoop versions, with default values
ARG spark_version=3.3.1
ARG hadoop_version=3

# Update package lists, install curl, download and extract Apache Spark, move it to /usr/bin/,
# create a logs directory, and then remove the downloaded archive
RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz

# Set environment variables for Spark, including SPARK_HOME, SPARK_MASTER_HOST, SPARK_MASTER_PORT, and PYSPARK_PYTHON
ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3

# -- Runtime

# Set the working directory to the Spark home directory
WORKDIR ${SPARK_HOME}