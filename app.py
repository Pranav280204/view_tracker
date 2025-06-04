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

# Configuration file
CONFIG_FILE = "config.json"

# Database setup
DB_FILE = "youtube_data.db"

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS video_stats (
                video_id TEXT,
                timestamp TEXT,
                view_count INTEGER,
                like_count INTEGER,
                comment_count INTEGER,
                PRIMARY KEY (video_id, timestamp)
            )
        """)
        conn.commit()

# Load or initialize API key and video IDs
def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    else:
        config = {
            "API_KEY": os.getenv("API_KEY", ""),  # Fallback to empty string if no API_KEY
            "VIDEO_IDS": []  # Initialize empty VIDEO_IDS list
        }
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
        return config

def save_config(video_ids, api_key=None):
    config = {
        "API_KEY": api_key if api_key is not None else load_config()["API_KEY"],
        "VIDEO_IDS": video_ids
    }
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f)

# Fetch YouTube data
def fetch_youtube_data():
    config = load_config()
    api_key = config["API_KEY"]
    video_ids = config["VIDEO_IDS"]
    
    if not api_key or not video_ids:
        print("API key or video IDs missing")
        return
    
    youtube = build("youtube", "v3", developerKey=api_key)
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        timestamp = datetime.datetime.now(pytz.UTC).isoformat()
        
        for video_id in video_ids:
            try:
                response = youtube.videos().list(
                    part="statistics",
                    id=video_id
                ).execute()
                
                if response.get("items"):
                    stats = response["items"][0]["statistics"]
                    view_count = int(stats.get("viewCount", 0))
                    like_count = int(stats.get("likeCount", 0))
                    comment_count = int(stats.get("commentCount", 0))
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO video_stats 
                        (video_id, timestamp, view_count, like_count, comment_count)
                        VALUES (?, ?, ?, ?, ?)
                    """, (video_id, timestamp, view_count, like_count, comment_count))
                    conn.commit()
            except Exception as e:
                print(f"Error fetching data for video {video_id}: {e}")

# Schedule data fetching
scheduler = BackgroundScheduler()
scheduler.add_job(
    fetch_youtube_data,
    trigger=CronTrigger(hour="*", minute=0),  # Run every hour
    timezone=pytz.UTC
)
scheduler.start()

# Flask routes
@app.route("/")
def index():
    config = load_config()
    return render_template("index.html", video_ids=config["VIDEO_IDS"])

@app.route("/update-video-ids", methods=["POST"])
def update_video_ids():
    data = request.get_json() or request.form
    new_video_ids = data.get("video_ids", "").split(",")
    new_video_ids = [vid.strip() for vid in new_video_ids if vid.strip()]  # Clean input
    
    # Basic validation for YouTube video IDs (11 characters, alphanumeric)
    valid_ids = [vid for vid in new_video_ids if len(vid) == 11 and vid.isalnum()]
    if not valid_ids and new_video_ids:
        return jsonify({"error": "Invalid video IDs provided"}), 400
    
    save_config(valid_ids)
    return jsonify({"message": "Video IDs updated successfully", "video_ids": valid_ids})

@app.route("/video-ids", methods=["GET"])
def get_video_ids():
    config = load_config()
    return jsonify({"video_ids": config["VIDEO_IDS"]})

@app.route("/stats", methods=["GET"])
def get_stats():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM video_stats ORDER BY timestamp DESC")
        rows = cursor.fetchall()
        stats = [
            {
                "video_id": row[0],
                "timestamp": row[1],
                "view_count": row[2],
                "like_count": row[3],
                "comment_count": row[4]
            }
            for row in rows
        ]
        return jsonify(stats)

if __name__ == "__main__":
    init_db()
    app.run(debug=True)
