# Sử dụng image Python 3.11.1
FROM python:3.11.1

# Cài đặt các thư viện cần thiết
RUN pip install kafka-python==2.0.2 pymongo==4.10.1 hdfs==2.7.3

# Tạo thư mục làm việc
WORKDIR /app

# Copy file consumer1.py vào container
COPY consumer1.py /app

# Chạy consumer1.py khi container khởi động
CMD ["python", "-u", "consumer1.py"]