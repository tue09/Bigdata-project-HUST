FROM python:3.11.1-slim

# Cài đặt Java
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk wget curl && \
    rm -rf /var/lib/apt/lists/*

# Thiết lập JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Cài đặt Apache Spark
ENV SPARK_VERSION=3.5.4
ENV HADOOP_VERSION=3
RUN wget https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Cài đặt các thư viện Python cần thiết
RUN pip install --no-cache-dir pymongo==4.10.1 hdfs==2.7.3 pyspark==3.5.4

# Tạo thư mục làm việc
WORKDIR /app

# Copy file consumer2.py vào container
COPY consumer2.py /app

# Chạy consumer2.py khi container khởi động
CMD ["spark-submit", "-u", "--conf", "spark.driver.bindAddress=0.0.0.0", "consumer2.py"]
