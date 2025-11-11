import time
import pandas as pd
import json
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# === Cấu hình ===
KAFKA_TOPIC = 'tweets_topic'
KAFKA_SERVER = 'kafka:9092'
DATA_PATH = '/app/data/airline_sentiment.csv' # Đường dẫn trong container
SLEEP_TIME = 0.1  # Giây

print("--- Bắt đầu script giả lập (THẬT) ---")

def get_kafka_producer(server):
    producer = None
    retries = 30
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[server],
                value_serializer=lambda v: json.dumps(v).encode('utf-8') # Gửi dưới dạng JSON
            )
            print(">>> Đã kết nối Kafka Producer!")
            return producer
        except NoBrokersAvailable:
            print(f"Không thể kết nối Kafka. Thử lại sau {SLEEP_TIME} giây...")
            retries -= 1
            time.sleep(SLEEP_TIME)
    
    print("!!! Không thể kết nối Kafka. Thoát.")
    return None

def run_simulator():
    producer = get_kafka_producer(KAFKA_SERVER)
    if producer is None:
        return

    try:
        print(f"Đọc dữ liệu từ: {DATA_PATH}")
        # Dùng pandas để đọc CSV dễ dàng
        df = pd.read_csv(DATA_PATH)
        print(f"Đã đọc {len(df)} dòng dữ liệu.")
        
        # Lặp vô tận để giả lập luồng (real-time)
        while True:
            for index, row in df.iterrows():
                # Đảm bảo các giá trị NaN (ví dụ: ở cột 'negativereason') được chuyển thành None
                message = row.where(pd.notnull(row), None).to_dict()
                
                producer.send(KAFKA_TOPIC, value=message)
                
                print(f"Gửi: {message['airline_sentiment']} | {message['airline']}")
                
                time.sleep(SLEEP_TIME)
                
            print("--- Đã gửi hết file, lặp lại từ đầu ---")
            time.sleep(10) # Chờ 10s trước khi lặp lại

    except FileNotFoundError:
        print(f"!!! LỖI: Không tìm thấy file {DATA_PATH}")
    except Exception as e:
        print(f"!!! LỖI trong quá trình chạy: {e}")
    finally:
        if producer:
            producer.close()
            print("--- Đã đóng Kafka Producer ---")

if __name__ == "__main__":
    run_simulator()