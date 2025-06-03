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

# Load or initialize API key and video ID
CONFIG_FILE = "config.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        config = {
            "API_KEY": os.getenv("API_KEY"),
            "VIDEO_ID": os.getenv("VIDEO_ID")
        }
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        return config

def save_config(api_key, video_id):
    config = {"API_KEY": api_key, "VIDEO_ID": video_id}
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f)

config = load_config()
API_KEY = config["API_KEY"]
VIDEO_ID = config["VIDEO_ID"]

# YouTube API setup
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def get_youtube_client():
    global API_KEY
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

youtube = get_youtube_client()

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
    global VIDEO_ID, youtube
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

@app.route("/update_config", methods=["POST"])
def update_config():
    global API_KEY, VIDEO_ID, youtube
    new_api_key = request.form.get("api_key")
    new_video_id = request.form.get("video_id")
    
    if not new_api_key or not new_video_id:
        return jsonify({"status": "error", "message": "Both API Key and Video ID are required"}), 400
    
    API_KEY = new_api_key
    VIDEO_ID = new_video_id
    save_config(new_api_key, new_video_id)
    youtube = get_youtube_client()
    return jsonify({"status": "success", "message": "API Key and Video ID updated successfully"})

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_and_store_views,
    trigger=CronTrigger(hour="0-9", minute="*/15", timezone="Asia/Kolkata")  # Every 15 minutes from 12 AM to 10 AM IST
)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True)
