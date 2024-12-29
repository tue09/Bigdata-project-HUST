import json
import time
import pymongo
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, LongType, FloatType
)
from pyspark.sql import functions as F
from hdfs import InsecureClient
from bson.json_util import dumps
from pymongo import UpdateOne
import os

# ---------------------------
# CẤU HÌNH
# ---------------------------

# HDFS
HDFS_URL = "hdfs://namenode:9000"
# HDFS_USER = "hieu0"
# HDFS_URL = os.environ.get('HDFS_URL')
# HDFS_USER = os.environ.get('HDFS_USER')
# HDFS_RAW_DATA_PATH = '/user/hieu0/twitter_data/raw'
HDFS_USER = 'root'
HDFS_RAW_DATA_PATH = '/twitter_data/raw'

# MongoDB
MONGO_URI = "mongodb://host.docker.internal:27017"
# MONGO_URI = os.environ.get('MONGO_URI')
MONGO_DB = "processed_data"

# Batch
BATCH_INTERVAL = 20  # seconds
BATCH_SIZE = 6

# ---------------------------
# HÀM KẾT NỐI
# ---------------------------

def connect_mongodb(db_name):
    """Kết nối tới MongoDB."""
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[db_name]
        print("Connected to MongoDB successfully!")
        return client, db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, None

def init_hdfs_client():
    """Khởi tạo HDFS client."""
    try:
        client = InsecureClient(f"http://namenode:9870", user=HDFS_USER)
        print("HDFS client initialized.")
        return client
    except Exception as e:
        print(f"Error initializing HDFS client: {e}")
        return None

# ---------------------------
# HÀM XỬ LÝ DỮ LIỆU
# ---------------------------

def get_new_files(hdfs_client, last_processed_timestamp):
    """Lấy danh sách các file mới từ HDFS dựa trên timestamp."""
    new_files = []
    for folder in ["projects_social_media", "tweets", "twitter_users"]:
        folder_path = f"{HDFS_RAW_DATA_PATH}/{folder}"
        try:
            for file_name in hdfs_client.list(folder_path):
                if file_name.endswith(".json"):
                    # Sửa đổi phần lấy timestamp ở đây
                    parts = file_name[:-5].split("_")  # Loại bỏ ".json" và sau đó split
                    if parts:
                        try:
                            file_timestamp = int(parts[-1]) # Lấy phần tử cuối cùng sau khi split
                            if file_timestamp > last_processed_timestamp:
                                new_files.append((file_timestamp, folder, file_name))
                        except ValueError:
                            print(f"Skipping file with invalid timestamp format: {file_name}")
                    else:
                        print(f"Skipping file with invalid name format: {file_name}")
        except Exception as e:
            print(f"Error listing files in {folder_path}: {e}")
    new_files.sort()
    return new_files

def read_data_from_hdfs(spark, hdfs_path, schema=None):
    """Đọc dữ liệu từ HDFS."""
    if schema:
        df = spark.read.schema(schema).json(hdfs_path)
    else:
        df = spark.read.json(hdfs_path)
    return df
    
def get_schema_for_topic(topic):
    """Định nghĩa schema cho từng topic (bỏ _id)."""
    if topic == "projects_social_media":
        return StructType([
            StructField("projectId", StringType(), True),
            StructField("twitter", StringType(), True),
            StructField("website", StringType(), True)
        ])
    elif topic == "tweets":
        return StructType([
            StructField("author", StringType(), True),
            StructField("authorName", StringType(), True),
            StructField("views", LongType(), True),
            StructField("likes", LongType(), True),
            StructField("replyCounts", LongType(), True),
            StructField("retweetCounts", LongType(), True)
        ])
    elif topic == "twitter_users":
        return StructType([
            StructField("_id", StringType(), True),
            StructField("userName", StringType(), True),
            StructField("url", StringType(), True),
            StructField("favouritesCount", IntegerType(), True),
            StructField("friendsCount", IntegerType(), True),
            StructField("listedCount", IntegerType(), True),
            StructField("mediaCount", IntegerType(), True),
            StructField("followersCount", IntegerType(), True),
            StructField("statusesCount", IntegerType(), True),
            StructField("blue", BooleanType(), True),
            StructField("location", StringType(), True)
        ])
    else:
        return None

# Schema cho trường twitter
twitter_schema = StructType([
    StructField("id", StringType(), True),
    StructField("url", StringType(), True)
])

def process_projects_and_users(spark, hdfs_client, batch_files, project_folder_collection):
    """Xử lý liên kết project và user."""
    project_urls = {}
    user_urls = {}

    for timestamp, folder, file_name in batch_files:
        if folder == "projects_social_media":
            hdfs_path = f"{HDFS_URL}{HDFS_RAW_DATA_PATH}/{folder}/{file_name}"
            schema = get_schema_for_topic(folder)
            df = read_data_from_hdfs(spark, hdfs_path, schema)
            if df is not None and not df.rdd.isEmpty():
                # Parse trường twitter từ string sang struct
                df = df.withColumn("twitter", F.from_json(df["twitter"], twitter_schema))
                
                projects = df.select("projectId", "twitter.url").collect()
                for project in projects:
                    if project.url:
                        project_urls[project.url] = project.projectId

        elif folder == "twitter_users":
            hdfs_path = f"{HDFS_URL}{HDFS_RAW_DATA_PATH}/{folder}/{file_name}"
            schema = get_schema_for_topic(folder)
            df = read_data_from_hdfs(spark, hdfs_path, schema)
            if df is not None and not df.rdd.isEmpty():
                users = df.select("_id", "url").collect()
                for user in users:
                    if user.url:
                        user_urls[user.url] = user._id

    ops = []
    for url, project_id in project_urls.items():
        if url in user_urls:
            user_id = user_urls[url]
            ops.append(
                UpdateOne(
                    {"projectId": project_id, "userId": user_id},
                    {"$set": {"projectId": project_id, "userId": user_id}},
                    upsert=True
                )
            )

    if ops:
        project_folder_collection.bulk_write(ops, ordered=False)
        print(f"Linked {len(ops)} users to projects in Project_folder")

def process_tweets_and_update_users(spark, hdfs_client, batch_files, preprocess_twitter_users_collection):
    """Xử lý tweets và cập nhật thông tin users."""
    user_updates = {}

    for timestamp, folder, file_name in batch_files:
        if folder == "tweets":
            hdfs_path = f"{HDFS_URL}{HDFS_RAW_DATA_PATH}/{folder}/{file_name}"
            schema = get_schema_for_topic(folder)
            df = read_data_from_hdfs(spark, hdfs_path, schema)
            if df is not None and not df.rdd.isEmpty():
                tweets = df.select("author", "views", "likes", "replyCounts", "retweetCounts").collect()
                for tweet in tweets:
                    author_id = tweet.author
                    if author_id not in user_updates:
                        user_updates[author_id] = {
                            "total_tweets": 0,
                            "total_views": 0,
                            "total_likes": 0,
                            "total_replyCounts": 0,
                            "total_retweetCounts": 0,
                            "max_views": 0,
                            "max_likes": 0,
                            "max_replyCounts": 0,
                            "max_retweetCounts": 0
                        }
                    user_updates[author_id]["total_tweets"] += 1
                    user_updates[author_id]["total_views"] += tweet.views or 0
                    user_updates[author_id]["total_likes"] += tweet.likes or 0
                    user_updates[author_id]["total_replyCounts"] += tweet.replyCounts or 0
                    user_updates[author_id]["total_retweetCounts"] += tweet.retweetCounts or 0
                    user_updates[author_id]["max_views"] = max(user_updates[author_id]["max_views"], tweet.views or 0)
                    user_updates[author_id]["max_likes"] = max(user_updates[author_id]["max_likes"], tweet.likes or 0)
                    user_updates[author_id]["max_replyCounts"] = max(user_updates[author_id]["max_replyCounts"], tweet.replyCounts or 0)
                    user_updates[author_id]["max_retweetCounts"] = max(user_updates[author_id]["max_retweetCounts"], tweet.retweetCounts or 0)
    
    ops = []
    for user_id, updates in user_updates.items():
        # Kiểm tra xem user đã tồn tại chưa
        existing_user = preprocess_twitter_users_collection.find_one({"_id": user_id})

        if existing_user:
            # Nếu user đã tồn tại, chỉ sử dụng $inc và $max
            ops.append(
                UpdateOne(
                    {"_id": user_id},
                    {
                        "$inc": {
                            "total_tweets": updates["total_tweets"],
                            "total_views": updates["total_views"],
                            "total_likes": updates["total_likes"],
                            "total_replyCounts": updates["total_replyCounts"],
                            "total_retweetCounts": updates["total_retweetCounts"]
                        },
                        "$max": {
                            "max_views": updates["max_views"],
                            "max_likes": updates["max_likes"],
                            "max_replyCounts": updates["max_replyCounts"],
                            "max_retweetCounts": updates["max_retweetCounts"]
                        }
                    }
                )
            )
        else:
            # Nếu user chưa tồn tại, thêm mới với giá trị ban đầu
            initial_data = {
                "_id": user_id,
                "total_tweets": updates["total_tweets"],
                "total_views": updates["total_views"],
                "total_likes": updates["total_likes"],
                "total_replyCounts": updates["total_replyCounts"],
                "total_retweetCounts": updates["total_retweetCounts"],
                "max_views": updates["max_views"],
                "max_likes": updates["max_likes"],
                "max_replyCounts": updates["max_replyCounts"],
                "max_retweetCounts": updates["max_retweetCounts"]
            }
            ops.append(
                UpdateOne(
                    {"_id": user_id},
                    {"$setOnInsert": initial_data},
                    upsert=True
                )
            )

    if ops:
        preprocess_twitter_users_collection.bulk_write(ops, ordered=False)
        print(f"Updated {len(ops)} users in preprocess_twitter_users")

def create_global_statistics(spark, hdfs_client, batch_files, general_users_collection):
    """Tạo thống kê chung toàn cục."""

    for timestamp, folder, file_name in batch_files:
        if folder == "twitter_users":
            hdfs_path = f"{HDFS_URL}{HDFS_RAW_DATA_PATH}/{folder}/{file_name}"
            schema = get_schema_for_topic(folder)
            df = read_data_from_hdfs(spark, hdfs_path, schema)
            if df is not None and not df.rdd.isEmpty():
                users = df.select("location", "blue").collect()
                for user in users:
                    # Xử lý location
                    location = user.location
                    if location:
                        general_users_collection.update_one(
                            {"_id": "location_stats"},
                            {"$inc": {f"data.{location}": 1}},
                            upsert=True
                        )

                    # Xử lý blue
                    blue_value = user.blue
                    if blue_value is not None:
                        if blue_value:
                            general_users_collection.update_one(
                                {"_id": "blue_stats"},
                                {"$inc": {"data.true": 1}},
                                upsert=True
                            )
                        else:
                            general_users_collection.update_one(
                                {"_id": "blue_stats"},
                                {"$inc": {"data.false": 1}},
                                upsert=True
                            )

    print("Updated location and blue statistics in general_users")

# ---------------------------
# MAIN LOGIC
# ---------------------------

def main():
    # 1) Khởi tạo Spark
    spark = SparkSession.builder.appName("BatchConsumer").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark session initialized.")

    # 2) Kết nối Mongo
    mongo_client, mongo_db = connect_mongodb(MONGO_DB)
    if mongo_db is None:
        spark.stop()
        return
    project_folder_collection = mongo_db["Project_folder"]
    preprocess_twitter_users_collection = mongo_db["preprocess_twitter_users"]
    general_users_collection = mongo_db["general_users"]
    batch_status_collection = mongo_db["batch_status"]

    # 3) Kết nối HDFS
    hdfs_client = init_hdfs_client()
    if hdfs_client is None:
        spark.stop()
        mongo_client.close()
        return

    last_processed_timestamp = 0

    while True:
        # Kiểm tra batch_status để xem có batch nào đang xử lý không
        last_batch = batch_status_collection.find_one({"status": "processing"})
        if last_batch:
            last_processed_timestamp = last_batch["_id"]
            processed_files = last_batch["processed_files"]
            print(f"Resuming batch from timestamp: {last_processed_timestamp}")
        else:
            processed_files = []
        new_files = get_new_files(hdfs_client, last_processed_timestamp)

        if new_files:
            # Tạo batch mới nếu không resume từ batch cũ
            if not last_batch:
                batch_timestamp = int(time.time())
                batch_status_collection.insert_one({
                    "_id": batch_timestamp,
                    "processed_files": [],
                    "status": "processing"
                })
                print(f"Started new batch with timestamp: {batch_timestamp}")
            else:
                batch_timestamp = last_processed_timestamp
                
            batch = [(ts, f, n) for ts, f, n in new_files if ts <= batch_timestamp 
                     and (f"{HDFS_RAW_DATA_PATH}/{f}/{n}" not in processed_files)][:BATCH_SIZE]

            if batch:
                print("Processing batch:", batch)
                
                # Xử lý liên kết project và user
                process_projects_and_users(spark, hdfs_client, batch, project_folder_collection)

                # Xử lý tweets và cập nhật thông tin users
                process_tweets_and_update_users(spark, hdfs_client, batch, preprocess_twitter_users_collection)

                # Cập nhật thống kê chung
                create_global_statistics(spark, hdfs_client, batch, general_users_collection)

                # Cập nhật trạng thái batch
                batch_status_collection.update_one(
                    {"_id": batch_timestamp},
                    {
                        "$set": {
                            "processed_files": [f"{HDFS_RAW_DATA_PATH}/{f}/{n}" for _, f, n in batch],
                            "status": "completed"
                        }
                    }
                )
                print(f"Batch completed: {batch_timestamp}")
                last_processed_timestamp = batch_timestamp
            else:
                print("No new files to process in this batch.")
                if last_batch and not new_files:  # If no new files and resuming, mark as completed
                    batch_status_collection.update_one(
                        {"_id": last_processed_timestamp},
                        {"$set": {"status": "completed"}}
                    )
                    print(f"Resumed batch completed: {last_processed_timestamp}")

        else:
            print("No new files found.")

        time.sleep(BATCH_INTERVAL)

if __name__ == "__main__":
    main()