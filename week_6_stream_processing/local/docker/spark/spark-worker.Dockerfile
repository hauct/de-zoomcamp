# Use the "spark-base" image as the base image
FROM spark-base

# -- Runtime

# Define an argument for the Spark Worker web UI port, with a default value of 8081
ARG spark_worker_web_ui=8081

# Expose the port specified by the Spark Worker web UI port
EXPOSE ${spark_worker_web_ui}

# Define the default command to run when a container is started, which starts the Spark Worker
CMD bin/spark-class org.apache.spark.deploy.worker.Worker spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} >> logs/spark-worker.out

