# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, when, count

# def run_spark_batch_job():
#     print("--- Bắt đầu Spark Batch Job ---")

#     spark = SparkSession.builder \
#         .appName("AirlineSentimentBatchAnalysis") \
#         .getOrCreate()

#     # --- 1. Cấu hình Postgres ---
#     # Thêm ?sslmode=disable để fix lỗi kết nối
#     postgres_url = "jdbc:postgresql://postgres:5432/bigdata_db?sslmode=disable"
#     postgres_table = "batch_results"
#     postgres_properties = {
#         "user": "admin",
#         "password": "admin",
#         "driver": "org.postgresql.Driver"
#     }

#     # --- 2. Đọc dữ liệu từ CSV ---
#     # Nó đọc từ đây (đã được mount vào)
#     data_path = "/app/data/airline_sentiment.csv"
#     try:
#         df = spark.read.csv(data_path, header=True, inferSchema=True)
#         print(f">>> Đã đọc {df.count()} dòng từ {data_path}")
#     except Exception as e:
#         print(f"!!! LỖI: Không thể đọc file CSV tại {data_path}. Lỗi: {e}")
#         spark.stop()
#         return

#     # --- 3. Xử lý (Transform) ---
#     # Logic này HOÀN TOÀN KHỚP với data của bạn
#     result_df = df.groupBy("airline").agg(
#         count(when(col("airline_sentiment") == "positive", 1)).alias("positive_count"),
#         count(when(col("airline_sentiment") == "negative", 1)).alias("negative_count"),
#         count(when(col("airline_sentiment") == "neutral", 1)).alias("neutral_count")
#     )

    
#     print(">>> Xử lý dữ liệu thành công. Kết quả (5 dòng đầu):")
#     result_df.show(5)

#     # --- 4. Ghi kết quả vào Postgres ---
#     try:
#         result_df.write \
#             .format("jdbc") \
#             .option("url", postgres_url) \
#             .option("dbtable", postgres_table) \
#             .option("user", postgres_properties["user"]) \
#             .option("password", postgres_properties["password"]) \
#             .option("driver", postgres_properties["driver"]) \
#             .mode("overwrite") \
#             .save()
        
#         print(f">>> Đã ghi kết quả vào bảng '{postgres_table}' thành công!")
        
#     except Exception as e:
#         print(f"!!! LỖI: Không thể ghi vào Postgres. Lỗi: {e}")

#     spark.stop()
#     print("--- Spark Batch Job hoàn thành ---")

# if __name__ == "__main__":
#     run_spark_batch_job()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

def run_spark_batch_job():
    print("--- Bắt đầu Spark Batch Job (Reading from MinIO) ---")

    # Cấu hình Spark cực kỹ cho MinIO
    spark = SparkSession.builder \
        .appName("AirlineSentimentBatchAnalysis") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1") \
        .getOrCreate()

    # --- 1. Cấu hình Postgres ---
    postgres_url = "jdbc:postgresql://postgres:5432/bigdata_db?sslmode=disable"
    postgres_table = "batch_results"
    postgres_properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    # --- 2. Đọc dữ liệu từ MinIO (S3) ---
    data_path = "s3a://datalake/airline_sentiment.csv"
    
    try:
        print(f">>> Đang đọc dữ liệu từ MinIO: {data_path}")
        # Thêm option header=True để nó nhận diện dòng đầu
        df = spark.read.csv(data_path, header=True, inferSchema=True)
        print(f">>> Đã đọc {df.count()} dòng từ MinIO.")
    except Exception as e:
        print(f"!!! LỖI: Không thể đọc file từ MinIO. Lỗi: {e}")
        spark.stop()
        return

    # --- 3. Xử lý ---
    result_df = df.groupBy("airline").agg(
        count(when(col("airline_sentiment") == "positive", 1)).alias("positive_count"),
        count(when(col("airline_sentiment") == "negative", 1)).alias("negative_count"),
        count(when(col("airline_sentiment") == "neutral", 1)).alias("neutral_count")
    )

    print(">>> Kết quả xử lý (5 dòng đầu):")
    result_df.show(5)

    # --- 4. Ghi vào Postgres ---
    try:
        result_df.write \
            .format("jdbc") \
            .option("url", postgres_url) \
            .option("dbtable", postgres_table) \
            .option("user", postgres_properties["user"]) \
            .option("password", postgres_properties["password"]) \
            .option("driver", postgres_properties["driver"]) \
            .mode("overwrite") \
            .save()
        print(f">>> Ghi DB thành công!")
    except Exception as e:
        print(f"!!! LỖI Ghi DB: {e}")

    spark.stop()

if __name__ == "__main__":
    run_spark_batch_job()