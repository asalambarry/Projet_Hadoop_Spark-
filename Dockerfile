FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install essential packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    bash coreutils procps openjdk-11-jdk python3 python3-pip wget curl git \
    net-tools iputils-ping nano gnupg lsb-release openssh-client openssh-server \
    supervisor sudo sshpass \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile.d/java.sh && \
    echo "export PATH=${JAVA_HOME}/bin:$PATH" >> /etc/profile.d/java.sh && \
    chmod +x /etc/profile.d/java.sh

# Install Hadoop
ENV HADOOP_VERSION=3.3.6
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

RUN wget -q https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm hadoop-${HADOOP_VERSION}.tar.gz && \
    mkdir -p $HADOOP_HOME/logs

# Configure Hadoop logging
RUN mkdir -p $HADOOP_CONF_DIR && \
    echo -e "log4j.rootLogger=INFO, console\n\
log4j.appender.console=org.apache.log4j.ConsoleAppender\n\
log4j.appender.console.layout=org.apache.log4j.PatternLayout\n\
log4j.appender.console.layout.ConversionPattern=%d{ISO8601} %-5p %c{2}:%L - %m%n\n\
log4j.logger.org.apache.hadoop.util.NativeCodeLoader=ERROR" > $HADOOP_CONF_DIR/log4j.properties

# Install Spark
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Install Kafka
ENV KAFKA_VERSION=3.7.2
ENV KAFKA_HOME=/opt/kafka
ENV PATH=$KAFKA_HOME/bin:$PATH

RUN wget -q https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz && \
    tar -xzf kafka_2.13-${KAFKA_VERSION}.tgz && \
    mv kafka_2.13-${KAFKA_VERSION} $KAFKA_HOME && \
    rm kafka_2.13-${KAFKA_VERSION}.tgz

# Create Kafka directories and copy configs
RUN mkdir -p $KAFKA_HOME/data $KAFKA_HOME/zookeeper
COPY config/kafka/ $KAFKA_HOME/config/

# Install MongoDB
RUN wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc | apt-key add - && \
    echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list && \
    apt-get update && \
    apt-get install -y mongodb-org && \
    mkdir -p /data/db && \
    chown -R root:root /data/db

# Install Python packages
COPY requirements.txt requirements-flask.txt /tmp/
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    pip3 install --no-cache-dir -r /tmp/requirements-flask.txt && \
    pip3 install --no-cache-dir kafka-python && \
    rm /tmp/requirements.txt /tmp/requirements-flask.txt

# Create Flask directory and copy files
RUN mkdir -p /opt/flask
WORKDIR /opt/flask
COPY app.py /opt/flask/
COPY templates/ /opt/flask/templates/
COPY static/ /opt/flask/static/
COPY start_flask.sh /opt/flask/
RUN chmod +x /opt/flask/start_flask.sh

# Create HDFS data directories
RUN mkdir -p /opt/hadoop_data/hdfs/namenode /opt/hadoop_data/hdfs/datanode && \
    chmod -R 755 /opt/hadoop_data

# Copy Hadoop configs
COPY config/hadoop/ $HADOOP_CONF_DIR/

# Setup SSH for Hadoop
RUN mkdir /var/run/sshd && \
    ssh-keygen -A && \
    echo "root:root" | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/UsePAM yes/UsePAM no/' /etc/ssh/sshd_config && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose necessary ports
EXPOSE 22 8888 9870 8088 8042 19888 9092 2181 5001

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
