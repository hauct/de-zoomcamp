# Use the "spark-base" image as the base image
FROM spark-base

# -- Runtime

# Define an argument for the Spark Master web UI port, with a default value of 8080
ARG spark_master_web_ui=8080

# Expose the ports specified by the Spark Master web UI port and SPARK_MASTER_PORT environment variable
EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}

# Define the default command to run when a container is started, which starts the Spark Master
CMD bin/spark-class org.apache.spark.deploy.master.Master >> logs/spark-master.out