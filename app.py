import sqlite3
import datetime
import os
import json
from flask import Flask, jsonify, render_template, request
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

app = Flask(__name__)

# Load or initialize API key and video IDs
CONFIG_FILE = "config.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        config = {
            "API_KEY": os.getenv("API_KEY"),
            "VIDEO_IDS": [os.getenv("VIDEO_ID")] if os.getenv("VIDEO_ID") else []
        }
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        return config

def save_config(video_ids):
    config = {
        "API_KEY": API_KEY,
        "VIDEO_IDS": video_ids
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f)

config = load_config()
API_KEY = config["API_KEY"]
VIDEO_IDS = config["VIDEO_IDS"]

# YouTube API setup
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

# SQLite setup
def init_db():
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT,
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

# Fetch views and store in database for all video IDs
def fetch_and_store_views():
    now = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
    for video_id in VIDEO_IDS:
        try:
            request = youtube.videos().list(
                part="statistics",
                id=video_id
            )
            response = request.execute()
            if not response["items"]:
                print(f"No data found for video ID: {video_id}")
                continue
            view_count = int(response["items"][0]["statistics"]["viewCount"])

            conn = sqlite3.connect("views.db")
            cursor = conn.cursor()
            
            # Get the last recorded view count for this video
            cursor.execute(
                "SELECT view_count FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 1",
                (video_id,)
            )
            last_view = cursor.fetchone()
            view_gain = 0 if last_view is None else view_count - last_view[0]
            
            # Store new data with date and video ID
            date_str = now.strftime("%Y-%m-%d")
            timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute(
                "INSERT INTO views (video_id, date, timestamp, view_count, view_gain) VALUES (?, ?, ?, ?, ?)",
                (video_id, date_str, timestamp_str, view_count, view_gain)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error fetching views for video {video_id}: {e}")

# Flask routes
@app.route("/")
def index():
    return render_template("index.html", video_ids=VIDEO_IDS)

@app.route("/api/views")
def get_views():
    today = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("SELECT video_id, timestamp, view_count, view_gain FROM views WHERE date = ? ORDER BY timestamp, video_id", (today,))
    data = [
        {"video_id": row[0], "timestamp": row[1], "view_count": row[2], "view_gain": row[3]}
        for row in cursor.fetchall()
    ]
    conn.close()
    return jsonify(data)

@app.route("/api/views/<date>")
def get_views_by_date(date):
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("SELECT video_id, timestamp, view_count, view_gain FROM views WHERE date = ? ORDER BY timestamp, video_id", (date,))
    data = [
        {"video_id": row[0], "timestamp": row[1], "view_count": row[2], "view_gain": row[3]}
        for row in cursor.fetchall()
    ]
    conn.close()
    return jsonify(data)

@app.route("/api/views/<date>/<video_id>")
def get_views_by_date_and_video(date, video_id):
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute(
        "SELECT video_id, timestamp, view_count, view_gain FROM views WHERE date = ? AND video_id = ? ORDER BY timestamp",
        (date, video_id)
    )
    data = [
        {"video_id": row[0], "timestamp": row[1], "view_count": row[2], "view_gain": row[3]}
        for row in cursor.fetchall()
    ]
    conn.close()
    return jsonify(data)

@app.route("/api/views/<date>/total")
def get_total_views_gained(date):
    video_id = request.args.get("video_id")
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    if video_id:
        cursor.execute(
            "SELECT video_id, SUM(view_gain) as total_gain FROM views WHERE date = ? AND video_id = ? GROUP BY video_id",
            (date, video_id)
        )
    else:
        cursor.execute(
            "SELECT video_id, SUM(view_gain) as total_gain FROM views WHERE date = ? GROUP BY video_id",
            (date,)
        )
    data = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    return jsonify(data)

@app.route("/add_video", methods=["POST"])
def add_video():
    global VIDEO_IDS
    new_video_id = request.form.get("video_id")
    
    if not new_video_id:
        return jsonify({"status": "error", "message": "Video ID is required"}), 400
    
    if new_video_id not in VIDEO_IDS:
        VIDEO_IDS.append(new_video_id)
        save_config(VIDEO_IDS)
        return jsonify({"status": "success", "message": "Video ID added successfully"})
    else:
        return jsonify({"status": "error", "message": "Video ID already exists"}), 400

@app.route("/remove_video", methods=["POST"])
def remove_video():
    global VIDEO_IDS
    video_id = request.form.get("video_id")
    
    if not video_id:
        return jsonify({"status": "error", "message": "Video ID is required"}), 400
    
    if video_id in VIDEO_IDS:
        VIDEO_IDS.remove(video_id)
        save_config(VIDEO_IDS)
        # Optional: Clean up database for this video ID
        conn = sqlite3.connect("views.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM views WHERE video_id = ?", (video_id,))
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "message": "Video ID removed successfully"})
    else:
        return jsonify({"status": "error", "message": "Video ID not found"}), 400

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_and_store_views,
    trigger=CronTrigger(hour="*", minute="0", timezone="Asia/Kolkata")  # Every hour, 24/7
)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True)
