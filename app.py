import sqlite3
import datetime
import os
import json
import re
from flask import Flask, jsonify, render_template, request
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import pytz

app = Flask(__name__)

# Configuration file
CONFIG_FILE = "config.json"
DB_FILE = "youtube_views.db"

def is_valid_youtube_id(video_id):
    """Validate YouTube video ID format (11 characters, alphanumeric with -_)."""
    return bool(re.match(r'^[a-zA-Z0-9_-]{11}$', video_id))

def load_config():
    """Load or initialize configuration from config.json."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        else:
            config = {
                "API_KEY": os.getenv("API_KEY"),
                "VIDEO_IDS": []
            }
            if not config["API_KEY"]:
                raise ValueError("API_KEY environment variable is not set")
            with open(CONFIG_FILE, "w") as f:
                json.dump(config, f)
            return config
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error loading config: {e}")
        return {"API_KEY": None, "VIDEO_IDS": []}

def save_config(video_ids):
    """Save video IDs to config.json."""
    try:
        config = {
            "API_KEY": os.getenv("API_KEY"),
            "VIDEO_IDS": video_ids
        }
        if not config["API_KEY"]:
            raise ValueError("API_KEY environment variable is not set")
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f)
    except (IOError, ValueError) as e:
        print(f"Error saving config: {e}")

def init_db():
    """Initialize SQLite database."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS video_views (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    view_count INTEGER,
                    timestamp DATETIME
                )
            """)
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error initializing database: {e}")

def fetch_video_views():
    """Fetch view counts for all video IDs and store in database."""
    config = load_config()
    if not config["API_KEY"] or not config["VIDEO_IDS"]:
        print("No API key or video IDs configured")
        return

    try:
        youtube = build('youtube', 'v3', developerKey=config["API_KEY"])
        request = youtube.videos().list(
            part="statistics",
            id=','.join(config["VIDEO_IDS"])
        )
        response = request.execute()

        valid_video_ids = []
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            timestamp = datetime.datetime.now(pytz.UTC).isoformat()
            for item in response.get("items", []):
                video_id = item["id"]
                valid_video_ids.append(video_id)
                view_count = int(item["statistics"].get("viewCount", 0))
                cursor.execute(
                    "INSERT INTO video_views (video_id, view_count, timestamp) VALUES (?, ?, ?)",
                    (video_id, view_count, timestamp)
                )
            conn.commit()

        # Remove invalid video IDs from config
        invalid_ids = [vid for vid in config["VIDEO_IDS"] if vid not in valid_video_ids]
        if invalid_ids:
            config["VIDEO_IDS"] = valid_video_ids
            save_config(config["VIDEO_IDS"])
            print(f"Removed invalid video IDs: {invalid_ids}")

    except Exception as e:
        print(f"Error fetching video views: {e}")
        if "quota" in str(e).lower():
            print("YouTube API quota exceeded. Please check your API key or quota limits.")

# Initialize database and scheduler
init_db()
scheduler = BackgroundScheduler(
    jobstores={'default': SQLAlchemyJobStore(url=f'sqlite:///{DB_FILE}')},
    timezone=pytz.UTC
)
scheduler.add_job(
    fetch_video_views,
    trigger=IntervalTrigger(minutes=1),  # Run every minute
    id='fetch_video_views',
    replace_existing=True
)
scheduler.start()

@app.route('/')
def index():
    """Render the main page with current video IDs."""
    config = load_config()
    return render_template('index.html', video_ids=config['VIDEO_IDS'])

@app.route('/add_video', methods=['POST'])
def add_video():
    """Add a video ID to the configuration."""
    try:
        data = request.get_json()
        video_id = data.get('video_id')
        if not video_id or not is_valid_youtube_id(video_id):
            return jsonify({'success': False, 'message': 'Invalid YouTube video ID'}), 400
        config = load_config()
        if video_id in config['VIDEO_IDS']:
            return jsonify({'success': False, 'message': 'Video ID already exists'}), 400
        config['VIDEO_IDS'].append(video_id)
        save_config(config['VIDEO_IDS'])
        return jsonify({'success': True, 'message': 'Video ID added successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error adding video ID: {str(e)}'}), 500

@app.route('/remove_video/<video_id>', methods=['POST'])
def remove_video(video_id):
    """Remove a video ID from the configuration."""
    try:
        if not is_valid_youtube_id(video_id):
            return jsonify({'success': False, 'message': 'Invalid YouTube video ID'}), 400
        config = load_config()
        if video_id in config['VIDEO_IDS']:
            config['VIDEO_IDS'].remove(video_id)
            save_config(config['VIDEO_IDS'])
            return jsonify({'success': True, 'message': 'Video ID removed successfully'})
        return jsonify({'success': False, 'message': 'Video ID not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error removing video ID: {str(e)}'}), 500

@app.route('/video_data/<video_id>')
def video_data(video_id):
    """Get view count data and view gains for a specific video ID."""
    try:
        if not is_valid_youtube_id(video_id):
            return jsonify({'success': False, 'message': 'Invalid YouTube video ID'}), 400
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # Limit to last 24 hours (1440 minutes) to manage data size
            cursor.execute(
                """
                SELECT timestamp, view_count 
                FROM video_views 
                WHERE video_id = ? 
                AND timestamp >= datetime('now', '-24 hours') 
                ORDER BY timestamp DESC
                """,
                (video_id,)
            )
            rows = cursor.fetchall()
            if not rows:
                return jsonify({'success': False, 'message': 'No data found for this video ID'}), 404
            
            # Calculate view gains
            data = []
            for i, row in enumerate(rows):
                timestamp, view_count = row
                view_gain = None
                if i < len(rows) - 1:  # Ensure there's a previous record
                    prev_view_count = rows[i + 1][1]
                    view_gain = view_count - prev_view_count
                data.append({
                    'timestamp': timestamp,
                    'view_count': view_count,
                    'view_gain': view_gain
                })
            # Reverse to return in chronological order (oldest to newest)
            data = data[::-1]
        return jsonify({'success': True, 'data': data})
    except sqlite3.Error as e:
        return jsonify({'success': False, 'message': f'Database error: {str(e)}'}), 500

if __name__ == '__main__':
    app.run(debug=True)
