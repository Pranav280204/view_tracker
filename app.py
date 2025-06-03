```python
import sqlite3
import datetime
import os
import json
from flask import Flask, jsonify, render_template, request
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration file
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
        "API_KEY": load_config()["API_KEY"],
        "VIDEO_IDS": video_ids
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f)

# Initialize database
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

# Fetch views and store in database for all video IDs
def fetch_and_store_views():
    config = load_config()
    video_ids = config["VIDEO_IDS"]
    if not video_ids:
        logger.info("No video IDs to fetch.")
        return

    youtube = build("youtube", "v3", developerKey=config["API_KEY"])
    try:
        # Fetch data for all video IDs in a single API call
        request = youtube.videos().list(
            part="statistics",
            id=",".join(video_ids)
        )
        response = request.execute()
        logger.info(f"API response: {response}")

        conn = sqlite3.connect("views.db")
        cursor = conn.cursor()
        india_tz = pytz.timezone("Asia/Kolkata")
        now = datetime.datetime.now(india_tz)
        date_str = now.strftime("%Y-%m-%d")
        timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S")

        for item in response.get("items", []):
            video_id = item["id"]
            view_count = int(item["statistics"].get("viewCount", 0))
            
            # Get the last recorded view count for this video
            cursor.execute(
                "SELECT view_count FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 1",
                (video_id,)
            )
            last_view = cursor.fetchone()
            view_gain = 0 if last_view is None else view_count - last_view[0]
            
            # Store new data
            cursor.execute(
                "INSERT INTO views (video_id, date, timestamp, view_count, view_gain) VALUES (?, ?, ?, ?, ?)",
                (video_id, date_str, timestamp_str, view_count, view_gain)
            )
            logger.info(f"Stored data for video_id: {video_id}, views: {view_count}, gain: {view_gain}")

        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error fetching data: {e}")

# Flask routes
@app.route("/")
def index():
    config = load_config()
    return render_template("index.html", video_ids=config["VIDEO_IDS"])

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
    config = load_config()
    video_ids = config["VIDEO_IDS"]
    new_video_id = request.form.get("video_id")
    
    if not new_video_id:
        return jsonify({"status": "error", "message": "Video ID is required"}), 400
    
    if new_video_id not in video_ids:
        video_ids.append(new_video_id)
        save_config(video_ids)
        logger.info(f"Added video ID: {new_video_id}")
        return jsonify({"status": "success", "message": "Video ID added successfully"})
    else:
        return jsonify({"status": "error", "message": "Video ID already exists"}), 400

@app.route("/remove_video", methods=["POST"])
def remove_video():
    config = load_config()
    video_ids = config["VIDEO_IDS"]
    video_id = request.form.get("video_id")
    
    if not video_id:
        return jsonify({"status": "error", "message": "Video ID is required"}), 400
    
    if video_id in video_ids:
        video_ids.remove(video_id)
        save_config(video_ids)
        conn = sqlite3.connect("views.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM views WHERE video_id = ?", (video_id,))
        conn.commit()
        conn.close()
        logger.info(f"Removed video ID: {video_id}")
        return jsonify({"status": "success", "message": "Video ID removed successfully"})
    else:
        return jsonify({"status": "error", "message": "Video ID not found"}), 400

# Scheduler setup
def start_scheduler():
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Kolkata"))
    trigger = CronTrigger(hour="*", minute="0")  # Every hour, all day
    scheduler.add_job(fetch_and_store_views, trigger)
    scheduler.start()
    logger.info("Scheduler started.")

if __name__ == "__main__":
    init_db()
    start_scheduler()
    app.run(debug=True)
```
