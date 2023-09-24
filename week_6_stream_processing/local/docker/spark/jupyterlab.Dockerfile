# Use the "cluster-base" image as the base image
FROM cluster-base

# -- Layer: JupyterLab

# Define arguments for Spark and JupyterLab versions, with default values
ARG spark_version=3.3.1
ARG jupyterlab_version=3.6.1

# Update package lists and install Python 3 pip, then install specific versions of PySpark and JupyterLab
RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    pip3 install wget pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# -- Runtime

# Expose port 8888 for JupyterLab
EXPOSE 8888

# Set the working directory to the shared workspace defined in the base image
WORKDIR ${SHARED_WORKSPACE}

# Define the default command to run when a container is started, which starts Jupyter Lab
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
