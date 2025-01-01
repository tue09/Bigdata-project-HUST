# Web3 KOL Twitter Account Quality Evaluation System

This project implements a system for evaluating the quality of Twitter accounts belonging to Key Opinion Leaders (KOLs) in the Web3 space. It leverages a Lambda architecture to collect, store, process, and visualize data, providing insights into the influence, effectiveness, engagement, and credibility of these accounts.

## Architecture

The system comprises the following key components:

*   **Data Crawling:** A producer component that extracts data from MongoDB and publishes it to dedicated Kafka topics.
*   **Storage:**
    *   **HDFS:** Stores raw data streamed from Kafka consumers.
    *   **MongoDB:** Stores preprocessed data for analysis and visualization.
*   **Analytics:**
    *   **Consumer 1 (Streaming):** Consumes data from Kafka topics, preprocesses it, stores raw data in HDFS, and processed data in MongoDB. It also identifies and stores KOLs based on calculated influence scores.
    *   **Consumer 2 (Batch):** Reads data from HDFS in batches, performs data linkage between projects and users, updates user information, and computes global statistics using Spark. It further add `engagementChangeLogs` for each twitter user.
*   **Visualization:** A Streamlit-based interactive dashboard for data exploration and analysis.

## Technologies Used

*   **Programming Language:** Python 3.11
*   **Messaging:** Apache Kafka
*   **Storage:** HDFS, MongoDB
*   **Processing:** Apache Spark, PySpark
*   **Visualization:** Streamlit
*   **Containerization:** Docker, Docker Compose

## Project Structure

```plaintext
project_folder/
├── docker-compose.yml        # Docker Compose configuration
├── producer/                 # Producer code and Dockerfile
│   ├── Dockerfile
│   └── producer.py
├── consumer1/                # Consumer 1 code and Dockerfile
│   ├── Dockerfile
│   └── consumer1.py
├── consumer2/                # Consumer 2 code and Dockerfile
│   ├── Dockerfile
│   └── consumer2.py
├── hadoop_conf/              # Hadoop configuration files
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   ├── mapred-site.xml
│   └── yarn-site.xml
├── spark/                    # Custom Dockerfile for Spark (procps installation)
│   └── Dockerfile
├── show.py                   # Streamlit application code
└── README.md                 # This README file
```
## Setup and Deployment

**Prerequisites:**

*   Docker
*   Docker Compose

**Steps:**

1. **Clone the repository:**

    ```bash
    git clone https://github.com/tue09/Bigdata-project-HUST.git
    ```

2. **Configuration (Optional):**
    *   Modify Hadoop configuration files in `hadoop_conf` if needed.
    *   Adjust environment variables in `docker-compose.yml` if required (e.g., `MONGO_URI`, `HDFS_URL`).

3. **Build and run the containers (excluding Streamlit):**

    ```bash
    docker-compose down -v
    docker-compose build
    docker-compose up -d
    ```

4. **Run Streamlit:**

    ```bash
    pip install streamlit pymongo pandas plotly
    streamlit run show.py
    ```

    Access the application at `http://localhost:8501`.

## Dashboard Features

The Streamlit dashboard provides the following functionalities:

*   **KOL Analysis:**
    *   View detailed information about individual KOLs.
    *   Filter KOLs by project.
    *   Display overall KOL statistics.
    *   Showcase the top 5 KOLs based on influence score.
*   **General Analytics:**
    *   Analyze user distribution by location.
    *   Examine the distribution of blue checkmarks.
    *   Present user engagement statistics.
*   **Data Query:**
    *   Query data directly from specific MongoDB collections.

## Contribution
Feel free to contribute