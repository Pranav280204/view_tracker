# app_viewer.py
from flask import Flask, render_template
from contextlib import contextmanager
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row
import os
import logging

# === Config ===
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
    "?sslmode=prefer"
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# === DB ===
@contextmanager
def get_db_cursor():
    conn = psycopg.connect(POSTGRES_URL, row_factory=dict_row)
    try:
        yield conn.cursor()
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

# === In-Memory Gain Calc (4 Columns) ===
def calc_gains(rows):
    if not rows:
        return []
    out = []
    for i, row in enumerate(rows):
        ts, views, date = row["timestamp"], row["views"], row["date"]
        gain = 0
        hourly = 0
        if i > 0 and rows[i-1]["date"] == date:
            gain = views - rows[i-1]["views"]
        try:
            now_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            hour_ago = (now_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            for prev in reversed(rows[:i]):
                if prev["date"] != date:
                    break
                if prev["timestamp"] <= hour_ago:
                    hourly = views - prev["views"]
                    break
        except:
            hourly = 0
        out.append((ts, views, gain, hourly))
    return out

# === Route ===
@app.route("/")
def viewer():
    videos = []
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT video_id, name FROM video_list WHERE is_tracking = 1 ORDER BY name")
            for row in cur.fetchall():
                vid = row["video_id"]
                name = row["name"]
                cur.execute("SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC", (vid,))
                dates = [r["date"] for r in cur.fetchall()]
                daily = {}
                for d in dates:
                    cur.execute("""
                        SELECT date, timestamp, views
                        FROM views WHERE video_id=%s AND date=%s
                        ORDER BY timestamp ASC
                    """, (vid, d))
                    daily[d] = calc_gains(cur.fetchall())
                videos.append({"video_id": vid, "name": name, "daily_data": daily})
        return render_template("viewer.html", videos=videos)
    except Exception as e:
        logging.error(f"Viewer error: {e}")
        return render_template("viewer.html", videos=[], error_message="Service unavailable.")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
