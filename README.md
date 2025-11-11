# project_bigdata

Cấu trúc: (mô tả ngắn)
- data/: chứa `airline_sentiment.csv`
- flink/: job PyFlink, Dockerfile, requirements
- spark/: job PySpark, Dockerfile, requirements
- simulator/: script giả lập gửi dữ liệu lên Kafka
- web_app/: Flask app + dashboard
- postgres/: init.sql để tạo bảng khi Postgres khởi động
- docker-compose.yml để khởi tạo stack (Postgres, Zookeeper, Kafka, simulator, flink, spark, web_app)

Hướng dẫn nhanh:
1. Sao chép file CSV gốc vào `project_bigdata/data/airline_sentiment.csv`.
2. Kiểm tra/cập nhật các `requirements.txt` cho phù hợp.
3. Build các image (hoặc dùng `docker-compose build`) và chạy `docker-compose up`.
4. Tinh chỉnh các script jobs (flink_stream_job.py, spark_batch_job.py, simulator.py, web_app/app.py) theo pipeline của bạn.

Lưu ý:
- Dockerfile trong `flink/` và `spark/` là ví dụ; triển khai Flink/Spark thật sự thường cần base image chính thức của Flink/Spark.
- Kafka image và zookeeper trong docker-compose là cấu hình đơn giản cho test; production cần cấu hình phức tạp hơn.
