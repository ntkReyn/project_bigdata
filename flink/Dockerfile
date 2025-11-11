# 1. BẮT ĐẦU VỚI IMAGE GỐC
FROM apache/flink:1.17.1-scala_2.12-java11

USER root

# 2. CÀI ĐẶT CÁC GÓI CẦN THIẾT (với ca-certificates)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# 3. FIX LỖI PYTHON (symlink)
RUN ln -s /usr/bin/python3 /usr/bin/python

# 4. FIX LỖI PYTHON (pip install)
RUN python -m pip install --upgrade pip
COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt

# 5. TẢI CÁC FILE .JAR (3 file)
# Jar cho Kafka (đã có)
RUN curl -L -o /opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar
# Jar cho Postgres (đã có)
RUN curl -L -o /opt/flink/lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# SỬA LỖI: THÊM JAR CONNECTOR JDBC CỦA FLINK (file còn thiếu)
RUN curl -L -o /opt/flink/lib/flink-connector-jdbc-3.1.2-1.17.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar

USER flink