# Reference from offical Apache Spark repository Dockerfile for Kubernetes
# https://github.com/apache/spark/blob/master/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/Dockerfile

# Define an argument for the Java image tag, defaulting to "17-jre"
ARG java_image_tag=17-jre

# Use the specified Java image tag as the base image
FROM eclipse-temurin:${java_image_tag}

# -- Layer: OS + Python

# Define an argument for the shared workspace directory, defaulting to "/opt/workspace"
ARG shared_workspace=/opt/workspace

# Create the specified shared workspace directory and perform several commands in a single RUN instruction
RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
    apt-get install -y python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Set an environment variable SHARED_WORKSPACE to the value of the shared workspace directory
ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

# Create a volume from the shared workspace directory
VOLUME ${shared_workspace}

# Define the default command to run when a container is started, in this case, it opens a bash shell
CMD ["bash"]