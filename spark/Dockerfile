# Sử dụng image Spark (PySpark) CHÍNH THỨC từ Apache
# FROM apache/spark-py:v3.5.1
FROM apache/spark:3.5.1

# Chuyển sang user root để cài đặt
USER root

# Image này rất tối giản, chúng ta cần cài 'curl' và 'ca-certificates'
# 'ca-certificates' rất quan trọng để curl https
RUN apt-get update && apt-get install -y curl ca-certificates && rm -rf /var/lib/apt/lists/*

# Cài đặt thư viện Python (psycopg2)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tải về Postgres JDBC Driver và đặt vào đúng thư mục jars của Spark
RUN curl -L -o /opt/spark/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Trở lại user mặc định của image (spark)
USER spark