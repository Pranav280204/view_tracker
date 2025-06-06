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

# Configuration
CONFIG_FILE = "config.json"
FIXED_VIDEO_ID = "YxWlaYCA8MU"  # Jhoome Jo Pathan
DATABASE = "video_views.db"

# Load or initialize API key and video IDs
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        config = {
            "API_KEY": os.getenv("API_KEY"),
            "VIDEO_IDS": [FIXED_VIDEO_ID, os.getenv("VIDEO_ID", "")]
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

# Initialize SQLite database
def init_db():
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS video_views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                video_id TEXT,
                view_count INTEGER,
                timestamp DATETIME
            )
        """)
        conn.commit()

# Fetch view count from YouTube Data API
def fetch_view_count(video_id):
    try:
        youtube = build("youtube", "v3", developerKey=API_KEY)
        request = youtube.videos().list(part="statistics", id=video_id)
        response = request.execute()
        if response["items"]:
            return int(response["items"][0]["statistics"]["viewCount"])
        return 0
    except Exception as e:
        print(f"Error fetching views for video {video_id}: {e}")
        return None

# Store view count in database
def store_view_count(video_id, view_count):
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO video_views (video_id, view_count, timestamp) VALUES (?, ?, ?)",
            (video_id, view_count, datetime.datetime.now(pytz.UTC))
        )
        conn.commit()

# Scheduled task to fetch and store views hourly
def fetch_and_store_views():
    config = load_config()
    video_ids = config["VIDEO_IDS"]
    for video_id in video_ids:
        if video_id:  # Skip empty video IDs
            view_count = fetch_view_count(video_id)
            if view_count is not None:
                store_view_count(video_id, view_count)
                print(f"Stored view count {view_count} for video {video_id}")

# Flask route to update the second video ID
@app.route("/update_video_id", methods=["POST"])
def update_video_id():
    data = request.get_json()
    new_video_id = data.get("video_id")
    if not new_video_id:
        return jsonify({"error": "Video ID is required"}), 400
    config = load_config()
    config["VIDEO_IDS"][1] = new_video_id  # Update second video ID
    save_config(config["VIDEO_IDS"])
    return jsonify({"message": "Video ID updated", "video_id": new_video_id})

# Flask route to get hourly view comparison
@app.route("/compare_views")
def compare_views():
    with sqlite3.connect(DATABASE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT video_id, view_count, timestamp
            FROM video_views
            WHERE video_id IN (?, ?)
            ORDER BY timestamp DESC
        """, (FIXED_VIDEO_ID, load_config()["VIDEO_IDS"][1]))
        rows = cursor.fetchall()

    # Process data for comparison
    comparison_data = []
    fixed_views = {}
    second_views = {}
    for row in rows:
        video_id, view_count, timestamp = row
        timestamp = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f%z")
        hour_key = timestamp.strftime("%Y-%m-%d %H:00:00")  # Group by hour
        if video_id == FIXED_VIDEO_ID:
            fixed_views[hour_key] = view_count
        else:
            second_views[hour_key] = view_count

    # Compare views for matching hours
    for hour in fixed_views:
        if hour in second_views:
            comparison_data.append({
                "hour": hour,
                "fixed_video_views": fixed_views[hour],
                "second_video_views": second_views.get(hour, 0),
                "difference": fixed_views[hour] - second_views.get(hour, 0)
            })

    return jsonify(comparison_data)

# Flask route to render comparison page
@app.route("/")
def index():
    return render_template("index.html")  # Create an HTML template for visualization

# Initialize configuration and database
API_KEY = load_config()["API_KEY"]
init_db()

# Set up scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_and_store_views,
    trigger=CronTrigger(hour="*", minute=0, second=0, timezone=pytz.UTC)
)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True)
