# Sử dụng image Python 3.11.1
FROM python:3.11.1

# Cài đặt các thư viện cần thiết
RUN pip install kafka-python==2.0.2 pymongo==4.10.1

# Tạo thư mục làm việc
WORKDIR /app

# Copy file producer.py vào container
COPY producer.py /app

# Chạy producer.py khi container khởi động
CMD ["python", "-u", "producer.py"]