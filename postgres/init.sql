/* TRÁCH NHIỆM CỦA THÀNH VIÊN 1
File này định nghĩa schema "giao kèo" (contract) giữa 3 thành viên.
*/

-- Bảng cho kết quả xử lý lô (Spark)
CREATE TABLE IF NOT EXISTS batch_results (
    airline TEXT PRIMARY KEY,
    positive_count INT,
    negative_count INT,
    neutral_count INT
);

-- Bảng cho kết quả xử lý luồng (Flink)
-- Thành viên 2 (Flink) sẽ ghi vào đây
CREATE TABLE IF NOT EXISTS stream_results (
    id SERIAL PRIMARY KEY,
    airline TEXT,
    sentiment TEXT,
    negativereason TEXT,
    count INT,
    window_time TIMESTAMP
);