FROM python:3.11.1

RUN pip install kafka-python==2.0.2 pymongo==4.10.1 hdfs==2.7.3

WORKDIR /app

COPY consumer1.py /app

CMD ["python", "-u", "consumer1.py"]