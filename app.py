import sqlite3
import datetime
import os
from flask import Flask, jsonify, render_template
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

app = Flask(__name__)

# YouTube API setup
API_KEY = os.getenv("API_KEY")
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
VIDEO_ID = os.getenv("VIDEO_ID")

# SQLite setup
def init_db():
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            timestamp TEXT,
            view_count INTEGER,
            view_gain INTEGER
        )
    """)
    conn.commit()
    conn.close()

# Initialize database on app startup
init_db()

# Fetch views and store in database
def fetch_and_store_views():
    now = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
    hour = now.hour
    # Run only between 12 AM and 10 AM IST
    if 0 <= hour < 10:
        try:
            request = youtube.videos().list(
                part="statistics",
                id=VIDEO_ID
            )
            response = request.execute()
            view_count = int(response["items"][0]["statistics"]["viewCount"])

            conn = sqlite3.connect("views.db")
            cursor = conn.cursor()
            
            # Get the last recorded view count
            cursor.execute("SELECT view_count FROM views ORDER BY timestamp DESC LIMIT 1")
            last_view = cursor.fetchone()
            view_gain = 0 if last_view is None else view_count - last_view[0]
            
            # Store new data with date
            date_str = now.strftime("%Y-%m-%d")
            timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                "INSERT INTO views (date, timestamp, view_count, view_gain) VALUES (?, ?, ?, ?)",
                (date_str, timestamp_str, view_count, view_gain)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error fetching views: {e}")

# Flask routes
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/views")
def get_views():
    today = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, view_count, view_gain FROM views WHERE date = ? ORDER BY timestamp", (today,))
    data = [
        {"timestamp": row[0], "view_count": row[1], "view_gain": row[2]}
        for row in cursor.fetchall()
    ]
    conn.close()
    return jsonify(data)

@app.route("/api/views/<date>")
def get_views_by_date(date):
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, view_count, view_gain FROM views WHERE date = ? ORDER BY timestamp", (date,))
    data = [
        {"timestamp": row[0], "view_count": row[1], "view_gain": row[2]}
        for row in cursor.fetchall()
    ]
    conn.close()
    return jsonify(data)

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_and_store_views,
    trigger=CronTrigger(hour="0-9", minute="*/5", timezone="Asia/Kolkata")
)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True)
