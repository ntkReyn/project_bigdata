from flask import Flask, render_template, jsonify, request
import psycopg2
import os

app = Flask(__name__)

# Lấy thông tin kết nối DB
def get_db_connection():
    conn = psycopg2.connect(
        host="postgres",
        database="bigdata_db",
        user="admin",
        password="admin"
    )
    return conn

# === Trang Chính (Render Dashboard) ===
@app.route('/')
def index():
    return render_template('index.html')

# === API 1: BATCH - Cột Ghép (Như cũ) ===
@app.route('/api/batch_grouped')
def api_batch_grouped():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT airline, positive_count, negative_count, neutral_count FROM batch_results ORDER BY (positive_count + negative_count + neutral_count) DESC")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        
        labels = []
        positive_data = []
        negative_data = []
        neutral_data = []
        
        for row in rows:
            labels.append(row[0])
            positive_data.append(row[1])
            negative_data.append(row[2])
            neutral_data.append(row[3])

        data = {
            "labels": labels,
            "datasets": [
                {"label": "Positive", "data": positive_data, "backgroundColor": 'rgba(75, 192, 192, 0.6)'},
                {"label": "Negative", "data": negative_data, "backgroundColor": 'rgba(255, 99, 132, 0.6)'},
                {"label": "Neutral", "data": neutral_data, "backgroundColor": 'rgba(201, 203, 207, 0.6)'}
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

# === API 2: BATCH - Biểu Đồ Tròn (API MỚI) ===
@app.route('/api/batch_totals')
def api_batch_totals():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Tính tổng của toàn bộ
        cur.execute("SELECT SUM(positive_count), SUM(negative_count), SUM(neutral_count) FROM batch_results")
        row = cur.fetchone()
        cur.close()
        conn.close()

        data = {
            "labels": ["Positive", "Negative", "Neutral"],
            "datasets": [
                {
                    "label": "Total Sentiment",
                    "data": [row[0], row[1], row[2]],
                    "backgroundColor": ['rgba(75, 192, 192, 0.6)', 'rgba(255, 99, 132, 0.6)', 'rgba(201, 203, 207, 0.6)'],
                    "hoverOffset": 4
                }
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})

# === API 3: STREAM - Biểu Đồ Đường (API MỚI) ===
@app.route('/api/stream_trend')
def api_stream_trend():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Lấy 30 window_time gần nhất, group trước rồi mới limit
        cur.execute("""
            SELECT * FROM (
                SELECT 
                    window_time,
                    TO_CHAR(window_time, 'HH24:MI:SS') as time_label,
                    SUM(CASE WHEN sentiment = 'positive' THEN count ELSE 0 END) as positive,
                    SUM(CASE WHEN sentiment = 'negative' THEN count ELSE 0 END) as negative
                FROM stream_results
                GROUP BY window_time
                ORDER BY window_time DESC
                LIMIT 30
            ) sub
            ORDER BY window_time ASC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        labels = [r[1] for r in rows]        # time_label
        positive_data = [r[2] for r in rows] # positive
        negative_data = [r[3] for r in rows] # negative

        data = {
            "labels": labels,
            "datasets": [
                {"label": "Positive Trend", "data": positive_data, "borderColor": 'rgba(75, 192, 192, 1)', "tension": 0.1, "fill": False},
                {"label": "Negative Trend", "data": negative_data, "borderColor": 'rgba(255, 99, 132, 1)', "tension": 0.1, "fill": False}
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})
    

@app.route('/api/stream_trend_by_airline')
def api_stream_trend_by_airline():
    try:
        airline = request.args.get("airline")

        conn = get_db_connection()
        cur = conn.cursor()

        # Lấy 30 window_time gần nhất cho hãng, group trước rồi mới limit
        cur.execute("""
            SELECT * FROM (
                SELECT 
                    window_time,
                    TO_CHAR(window_time, 'HH24:MI:SS') AS time_label,
                    SUM(CASE WHEN sentiment='positive' THEN count ELSE 0 END) AS positive,
                    SUM(CASE WHEN sentiment='negative' THEN count ELSE 0 END) AS negative
                FROM stream_results
                WHERE airline = %s
                GROUP BY window_time
                ORDER BY window_time DESC
                LIMIT 30
            ) sub
            ORDER BY window_time ASC;
        """, (airline,))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        data = {
            "labels": [r[1] for r in rows],        # time_label
            "datasets": [
                {"label": "Positive", "data": [r[2] for r in rows]},
                {"label": "Negative", "data": [r[3] for r in rows]}
            ]
        }
        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)})
    
@app.route('/api/airlines')
def api_airlines():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT airline FROM stream_results ORDER BY airline ASC")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([row[0] for row in rows])
    except Exception as e:
        return jsonify({"error": str(e)})



# === API 4: STREAM - Cột 10 Cửa Sổ (API Cũ, đổi tên cho rõ) ===
@app.route('/api/stream_latest_windows')
def api_stream_latest_windows():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT airline, sentiment, count, TO_CHAR(window_time, 'HH24:MI:SS') as time 
            FROM stream_results 
            ORDER BY window_time DESC 
            LIMIT 10
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        labels = []
        data_counts = []
        colors = []
        
        for row in reversed(rows): # Đảo ngược
            labels.append(f"{row[0]} ({row[1]}) @ {row[3]}")
            data_counts.append(row[2])
            if row[1] == 'positive':
                colors.append('rgba(75, 192, 192, 0.6)')
            elif row[1] == 'negative':
                colors.append('rgba(255, 99, 132, 0.6)')
            else:
                colors.append('rgba(201, 203, 207, 0.6)')

        data = {
            "labels": labels,
            "datasets": [
                {
                    "label": "Real-time Sentiment Count (Last 10 windows)",
                    "data": data_counts,
                    "backgroundColor": colors
                }
            ]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)})
    
# === API 5: STREAM - Cột ngang theo Negative Reason (Realtime) ===
@app.route('/api/stream_negativereasons')
def api_stream_negativereasons():
    try:
        airline = request.args.get("airline")  # có thể rỗng để lấy tất cả

        conn = get_db_connection()
        cur = conn.cursor()

        query = """
            SELECT negativereason, SUM(count) as total_count
            FROM stream_results
        """
        params = []

        if airline:
            query += " WHERE airline = %s"
            params.append(airline)

        query += " GROUP BY negativereason ORDER BY total_count DESC"

        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        labels = [r[0] if r[0] else "Unknown" for r in rows]
        data_counts = [r[1] for r in rows]

        return jsonify({
            "labels": labels,
            "datasets": [
                {
                    "label": "Số lượng lý do Negative",
                    "data": data_counts,
                    "backgroundColor": "rgba(255, 99, 132, 0.6)"
                }
            ]
        })

    except Exception as e:
        return jsonify({"error": str(e)})


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)