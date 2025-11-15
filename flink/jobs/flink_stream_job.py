from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble

def run_flink_stream_job():
    print("--- Bắt đầu Flink Stream Job ---")

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 2. NGUỒN KAFKA
    t_env.execute_sql("""
        CREATE TABLE tweets_source (
            `airline_sentiment` STRING,
            `negativereason` STRING,
            `airline` STRING,
            `text` STRING,
            `proc_time` AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'tweets_topic',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink_consumer_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # 3. ĐÍCH POSTGRES
    t_env.execute_sql("""
        CREATE TABLE stream_results_sink (
            `airline` STRING,
            `sentiment` STRING,
            `negativereason` STRING,
            `count` BIGINT,
            `window_time` TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/bigdata_db?sslmode=disable',
            'table-name' = 'stream_results',
            'username' = 'admin',
            'password' = 'admin',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # 4. Tính toán cửa sổ
    tweets_table = t_env.from_path("tweets_source")

    windowed_counts = tweets_table.filter(
        col("airline_sentiment").is_not_null
    ).window(
        Tumble.over(lit(10).seconds).on(col("proc_time")).alias("w")
    ).group_by(
        col("w"), col("airline"), col("airline_sentiment"), col("negativereason")
    ).select(
        col("airline"),
        col("airline_sentiment").alias("sentiment"),
        col("negativereason").alias("negativereason"),
        col("airline_sentiment").count.alias("count"),
        col("w").end.alias("window_time")
    )

    print(">>> Đã định nghĩa Flink Job. Bắt đầu chạy...")

    windowed_counts.execute_insert("stream_results_sink").wait()


if __name__ == "__main__":
    run_flink_stream_job()