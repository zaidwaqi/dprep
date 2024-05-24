# Use the official Python 3.11 slim image from the Docker Hub
FROM python:3.11-slim

# Set environment variables for Hadoop
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
ENV CPATH=/usr/include/tirpc:$CPATH

# Install essential tools and libraries
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    curl \
    default-jdk \
    gnupg2 \
    dirmngr \
    gcc \
    g++ \
    libkrb5-dev \
    libsasl2-dev \
    python3-dev \
    make \
    libtirpc-dev \
    libnsl-dev \
    rpcbind \
    libncurses5-dev \
    libncursesw5-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz && \
    mkdir -p $HADOOP_HOME && \
    tar -xzf /tmp/hadoop.tar.gz -C $HADOOP_HOME --strip-components=1 && \
    rm /tmp/hadoop.tar.gz

# Determine JAVA_HOME dynamically and update Hadoop configuration
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "export JAVA_HOME=$JAVA_HOME" >> /etc/profile && \
    echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Install pydoop
RUN pip install pydoop

# Verify the installation
RUN python -c "import pydoop"

# Set the working directory
WORKDIR /workspace

# Expose necessary ports (if any)
EXPOSE 9870 8088 8080 50070 50075 50010 50020 50030

# Default command
CMD ["bash"]
