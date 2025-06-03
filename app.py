import sqlite3
import datetime
import os
import json
import re
from flask import Flask, jsonify, render_template, request
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

app = Flask(__name__)

# Load or initialize API key and video data
CONFIG_FILE = "config.json"

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        video_id = os.getenv("VIDEO_ID")
        config = {
            "API_KEY": os.getenv("API_KEY"),
            "VIDEOS": []
        }
        if video_id:
            # Fetch title for initial video ID
            try:
                request = youtube.videos().list(
                    part="snippet",
                    id=video_id
                )
                response = request.execute()
                title = response["items"][0]["snippet"]["title"] if response["items"] else "Unknown Title"
                config["VIDEOS"].append({"id": video_id, "title": title})
            except Exception as e:
                print(f"Error fetching title for initial video ID {video_id}: {e}")
                config["VIDEOS"].append({"id": video_id, "title": "Unknown Title"})
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        return config

def save_config(videos):
    config = {
        "API_KEY": API_KEY,
        "VIDEOS": videos
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f)

config = load_config()
API_KEY = config["API_KEY"]
VIDEOS = config["VIDEOS"]

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

# Fetch views and store in database for all videos
def fetch_and_store_views():
    now = datetime.datetime.now(pytz.timezone("Asia/Kolkata"))
    hour = now.hour
    # Run only between 12 AM and 11 AM IST
    if 0 <= hour < 11:
        for video in VIDEOS:
            video_id = video["id"]
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
    return render_template("index.html", videos=VIDEOS)

@app.route("/api/views")
def get_views():
    today = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%Y-%m-%d")
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("SELECT video_id, timestamp, view_count, view_gain FROM views WHERE date = ? ORDER BY timestamp, video_id", (today,))
    data = []
    for row in cursor.fetchall():
        video_id = row[0]
        title = next((video["title"] for video in VIDEOS if video["id"] == video_id), "Unknown Title")
        data.append({"video_id": video_id, "title": title, "timestamp": row[1], "view_count": row[2], "view_gain": row[3]})
    conn.close()
    return jsonify(data)

@app.route("/api/views/<date>")
def get_views_by_date(date):
    conn = sqlite3.connect("views.db")
    cursor = conn.cursor()
    cursor.execute("SELECT video_id, timestamp, view_count, view_gain FROM views WHERE date = ? ORDER BY timestamp, video_id", (date,))
    data = []
    for row in cursor.fetchall():
        video_id = row[0]
        title = next((video["title"] for video in VIDEOS if video["id"] == video_id), "Unknown Title")
        data.append({"video_id": video_id, "title": title, "timestamp": row[1], "view_count": row[2], "view_gain": row[3]})
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
    data = []
    for row in cursor.fetchall():
        title = next((video["title"] for video in VIDEOS if video["id"] == row[0]), "Unknown Title")
        data.append({"video_id": row[0], "title": title, "timestamp": row[1], "view_count": row[2], "view_gain": row[3]})
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
    data = {}
    for row in cursor.fetchall():
        video_id = row[0]
        title = next((video["title"] for video in VIDEOS if video["id"] == video_id), "Unknown Title")
        data[title] = row[1]
    conn.close()
    return jsonify(data)

@app.route("/add_video", methods=["POST"])
def add_video():
    global VIDEOS
    video_link = request.form.get("video_link")
    
    if not video_link:
        return jsonify({"status": "error", "message": "Video link is required"}), 400
    
    # Extract video ID from the link
    video_id = None
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11}).*",
        r"youtu\.be\/([0-9A-Za-z_-]{11})"
    ]
    for pattern in patterns:
        match = re.search(pattern, video_link)
        if match:
            video_id = match.group(1)
            break
    
    if not video_id:
        return jsonify({"status": "error", "message": "Invalid YouTube video link"}), 400
    
    if video_id in [video["id"] for video in VIDEOS]:
        return jsonify({"status": "error", "message": "Video is already being tracked"}), 400
    
    # Fetch video title
    try:
        request = youtube.videos().list(
            part="snippet",
            id=video_id
        )
        response = request.execute()
        if not response["items"]:
            return jsonify({"status": "error", "message": "Video not found"}), 400
        title = response["items"][0]["snippet"]["title"]
    except Exception as e:
        print(f"Error fetching title for video ID {video_id}: {e}")
        title = "Unknown Title"
    
    VIDEOS.append({"id": video_id, "title": title})
    save_config(VIDEOS)
    return jsonify({"status": "success", "message": "Video added successfully", "title": title, "video_id": video_id})

@app.route("/remove_video", methods=["POST"])
def remove_video():
    global VIDEOS
    video_id = request.form.get("video_id")
    
    if not video_id:
        return jsonify({"status": "error", "message": "Video ID is required"}), 400
    
    video = next((v for v in VIDEOS if v["id"] == video_id), None)
    if video:
        VIDEOS.remove(video)
        save_config(VIDEOS)
        # Clean up database for this video ID
        conn = sqlite3.connect("views.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM views WHERE video_id = ?", (video_id,))
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "message": "Video removed successfully", "title": video["title"]})
    else:
        return jsonify({"status": "error", "message": "Video not found"}), 400

# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_and_store_views,
    trigger=CronTrigger(hour="0-10", minute="0", timezone="Asia/Kolkata")  # Every hour from 12 AM to 11 AM IST
)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True)
