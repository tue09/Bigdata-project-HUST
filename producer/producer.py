import os
import pymongo
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
from bson.json_util import dumps

# MONGO_URI = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017"
# MONGO_URI = "mongodb://root:password@mongodb:27017"
# MONGO_URI = "mongodb://root:password@host.docker.internal:27017"
MONGO_URI = "mongodb://host.docker.internal:27017"
# MONGO_URI = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@host.docker.internal:27017"
# MONGO_URI = f"mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@172.18.0.2.:27017"
#MONGO_URI = "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@mongodb:27017"
MONGO_DB = "cdp_database"  # Thay tên database nếu cần
MONGO_CLIENT = pymongo.MongoClient(MONGO_URI)

#KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(',')
# KAFKA_BOOTSTRAP_SERVERS = [os.environ.get('KAFKA_BOOTSTRAP_SERVERS')]  # Thay bằng địa chỉ server của Kafka
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_TOPICS = {
    "projects_social_media": "projects_social_media_topic",
    "tweets": "tweets_topic",
    "twitter_users": "twitter_users_topic"
}

def connect_mongodb(db_name):
    try:
        print(f"Attempting to connect to MongoDB at: {MONGO_URI}")  # In ra URI đang sử dụng
        MONGO_CLIENT.admin.command('ping')  # Thử ping server MongoDB
        db = MONGO_CLIENT[db_name]
        print("Connected to MongoDB successfully!")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def initialize_kafka_producer(retries=10, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Encode to bytes
            )
            print("Kafka producer initialized.")
            return producer
        except NoBrokersAvailable as e:
            attempt += 1
            print(f"Attempt {attempt}/{retries}: Kafka not available, retrying in {delay} seconds...")
            time.sleep(delay)
    print("Failed to connect to Kafka after multiple attempts.")
    return None

def fetch_data_from_mongodb(db, collection_name):
    collection = db[collection_name]
    return collection.find()

def send_message_to_kafka(producer, topic, message):
    try:
        producer.send(topic, value=message)
        producer.flush()  # Make sure the message is sent
        print(f"Message {message} sent to topic {topic}")
    except Exception as e:
        print(f"Error sending message to Kafka topic {topic}: {e}")

def main():
    print('START Producer')
    db = connect_mongodb(MONGO_DB)
    if db is None:
        return

    producer = initialize_kafka_producer()
    if producer is None:
        return

    collection_names = list(KAFKA_TOPICS.keys())

    cursors = {}
    for name in collection_names:
        cursors[name] = fetch_data_from_mongodb(db, name)

    try:
        while True:  
            for collection_name in collection_names:
                try:
                    cursor = cursors[collection_name]
                    message = next(cursor, None)
                    if message:
                        topic_name = KAFKA_TOPICS[collection_name]
                        send_message_to_kafka(producer, topic_name, message)
                    else:
                        cursors[collection_name] = fetch_data_from_mongodb(db, collection_name)
                        print(f"Reset cursor for collection {collection_name}")
                except StopIteration:
                    print(f"Collection {collection_name} has been traversed completely. Resetting.")
                    cursors[collection_name] = fetch_data_from_mongodb(db, collection_name)
            time.sleep(10)  

    except KeyboardInterrupt:
        print("Producer is terminated by the user.")
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    main()
