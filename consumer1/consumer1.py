from kafka import KafkaConsumer
from pymongo import MongoClient, UpdateOne
from hdfs import InsecureClient
import json
import time
import getpass
import os

print(f"Running as user: {getpass.getuser()}")

KAFKA_BROKER = 'kafka:9092'
#KAFKA_BROKER = os.environ.get('KAFKA_BROKER')

KAFKA_TOPICS = {
    "projects_social_media_topic": "projects_social_media",
    "tweets_topic": "tweets",
    "twitter_users_topic": "twitter_users"
}
KAFKA_GROUP_ID = 'group-id-2'

MONGO_URI = "mongodb://host.docker.internal:27017"
# MONGO_URI = os.environ.get('MONGO_URI')
MONGO_DB_NAME = 'processed_data'
MONGO_COLLECTIONS = {
    "projects_social_media": "preprocess_projects_social_media",
    "tweets": "preprocess_tweets",
    "twitter_users": "preprocess_twitter_users"
}
MONGO_COLLECTION_KOL = 'KOL_folder'

HDFS_URL = 'http://namenode:9870'
# HDFS_URL = os.environ.get('HDFS_URL')
# HDFS_USER = os.environ.get('HDFS_USER')
HDFS_USER = 'root'
HDFS_BASE_PATH = '/twitter_data/raw'


# Ngưỡng để xác định KOL
KOL_INFLUENCE_THRESHOLD = 50

consumer = KafkaConsumer(
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=KAFKA_GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
consumer.subscribe(topics=list(KAFKA_TOPICS.keys()))

MONGO_CLIENT = MongoClient(MONGO_URI)
MONGO_DB = MONGO_CLIENT[MONGO_DB_NAME]

HDFS_CLIENT = InsecureClient(HDFS_URL, user=HDFS_USER)

def calculate_influence_score(followers_count, friends_count, statuses_count):
    followers_count = followers_count if followers_count is not None else 0
    friends_count = friends_count if friends_count is not None else 0
    statuses_count = statuses_count if statuses_count is not None else 0
    return (followers_count * 0.5) + (statuses_count * 0.3) - (friends_count * 0.2)

def replace_twitter_with_x_recursive(data):
    if isinstance(data, dict):
        return {k: replace_twitter_with_x_recursive(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_twitter_with_x_recursive(v) for v in data]
    elif isinstance(data, str):
        return data.replace("twitter", "x").replace("Twitter", "X")
    else:
        return data

def preprocess_data(data, topic_name):
    data = replace_twitter_with_x_recursive(data)
    processed_data = {}

    if topic_name == "projects_social_media":
        processed_data = {
            '_id': data.get('_id'),
            'projectId': data.get('projectId'),
            'twitter': data.get('twitter'),
            'website': data.get('website'),
            'timestamp': int(time.time())
        }
    elif topic_name == "tweets":
        processed_data = {
            '_id': data.get('_id'),
            'author': data.get('author'),
            'authorName': data.get('authorName'),
            'views': data.get('views'),
            'likes': data.get('likes'),
            'replyCounts': data.get('replyCounts'),
            'retweetCounts': data.get('retweetCounts'),
            'timestamp': int(time.time())
        }
    elif topic_name == "twitter_users":
        influence_score = calculate_influence_score(data.get('followersCount'), data.get('friendsCount'), data.get('statusesCount'))
        processed_data = {
            '_id': data.get('_id'),
            'userName': data.get('userName'),
            'url': data.get('url'),
            'favouritesCount': data.get('favouritesCount'),
            'friendsCount': data.get('friendsCount'),
            'listedCount': data.get('listedCount'),
            'mediaCount': data.get('mediaCount'),
            'followersCount': data.get('followersCount'),
            'statusesCount': data.get('statusesCount'),
            'blue': data.get('blue'),
            'influence_score': influence_score,
            'timestamp': int(time.time())
        }
    return processed_data

def save_to_hdfs(data, hdfs_client, hdfs_path):
    timestamp = int(time.time())
    filename = f"{data.get('topic')}_{timestamp}.json"
    filepath = f"{hdfs_path}/{filename}"
    try:
        with hdfs_client.write(filepath, overwrite=True, encoding='utf-8') as writer:
            json.dump(data, writer)
        print(f"Saved raw data to HDFS: {filepath}")
    except Exception as e:
        print(f"Error saving to HDFS: {e}")

def save_to_mongodb(data, collection_name):
    try:
        collection = MONGO_DB[collection_name]
        existing_document = collection.find_one({'_id': data['_id']})
        if existing_document is None:
            collection.insert_one(data)
            print(f"Saved processed data to MongoDB collection {collection_name}: {data['_id']}")
        else:
            print(f"Document with _id {data['_id']} already exists in MongoDB collection {collection_name}. Skipping.")
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

def identify_and_save_kol(data, processed_data, collection, threshold):
    if processed_data.get('influence_score', 0) >= threshold:
        try:
            kol_data = data.copy()
            kol_data.update({
                'influence_score': processed_data['influence_score'],
                'identified_at': int(time.time()),
                'source': 'real-time'
            })
            existing_kol = collection.find_one({'_id': kol_data['_id']})
            if existing_kol is None:
                collection.insert_one(kol_data)
                print(f"Identified and saved new KOL to MongoDB: {kol_data['_id']}")
            else:
                collection.update_one({'_id': kol_data['_id']}, {'$set': kol_data})
                print(f"Updated existing KOL in MongoDB: {kol_data['_id']}")
        except Exception as e:
            print(f"Error saving KOL to MongoDB: {e}")

print(f'Start Consumer 1 ...')
for message in consumer:
    topic_name = message.topic
    data = message.value
    data['topic'] = KAFKA_TOPICS[topic_name] # Thêm topic name vào data để lưu vào HDFS
    print("-------------------------------------------------")
    print(f"Received from Kafka topic {topic_name}: {data.get('_id', 'N/A')}")

    processed_data = preprocess_data(data, KAFKA_TOPICS[topic_name])

    hdfs_path = f"{HDFS_BASE_PATH}/{KAFKA_TOPICS[topic_name]}"
    save_to_hdfs(data, HDFS_CLIENT, hdfs_path)

    if processed_data:
        save_to_mongodb(processed_data, MONGO_COLLECTIONS[KAFKA_TOPICS[topic_name]])

    if topic_name == "twitter_users_topic":
        identify_and_save_kol(data, processed_data, MONGO_DB[MONGO_COLLECTION_KOL], KOL_INFLUENCE_THRESHOLD)
    print("-------------------------------------------------")