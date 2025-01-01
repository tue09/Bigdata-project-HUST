FROM python:3.11.1

RUN pip install kafka-python==2.0.2 pymongo==4.10.1

WORKDIR /app

COPY producer.py /app

CMD ["python", "-u", "producer.py"]