FROM python:3.11.1-slim

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

RUN pip install --no-cache-dir pymongo==4.10.1 hdfs==2.7.3 pyspark==3.5.4

WORKDIR /app

COPY consumer2.py /app

CMD ["spark-submit", "-u", "--conf", "spark.driver.bindAddress=0.0.0.0", "consumer2.py"]
