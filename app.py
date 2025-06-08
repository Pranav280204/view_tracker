import os
import threading
import logging
import pytz
import sqlite3
import time
from datetime import datetime, timedelta
import pandas as pd
from flask import Flask, render_template, send_file, request, redirect, url_for
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# YouTube API setup
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY environment variable is not set")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# SQLite database setup
def init_db():
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        # Table for video views
        c.execute("""CREATE TABLE IF NOT EXISTS views (
            video_id TEXT,
            timestamp TEXT,
            views INTEGER
        )""")
        # Table for video list (ID, name, targetable flag)
        c.execute("""CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_targetable INTEGER  -- 1 for targetable, 0 for not
        )""")
        # Table for target settings per video
        c.execute("""CREATE TABLE IF NOT EXISTS targets (
            video_id TEXT PRIMARY KEY,
            target_views INTEGER,
            target_time TEXT,
            required_views_per_interval REAL
        )""")
        conn.commit()
        logger.info("Successfully initialized database")

        # Initialize with default videos if the table is empty
        c.execute("SELECT COUNT(*) FROM video_list")
        if c.fetchone()[0] == 0:
            default_videos = [
                ("hTSaweR8qMI", "MrBeast", 1),  # Targetable
                ("hxMNYkLN7tI", "Aj Ki Raat", 0),
                ("ekr2nIex040", "Rose", 0)
            ]
            c.executemany("INSERT INTO video_list (video_id, name, is_targetable) VALUES (?, ?, ?)", default_videos)
            conn.commit()
            logger.info("Initialized default videos")
    except sqlite3.Error as e:
        logger.error(f"Database initialization failed: {e}")
    finally:
        conn.close()

# Fetch views for multiple video IDs
def fetch_views(video_ids):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return {}
    try:
        response = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        views = {}
        for item in response.get("items", []):
            video_id = item["id"]
            views[video_id] = int(item["statistics"]["viewCount"])
        return views
    except HttpError as e:
        logger.error(f"Error fetching views for {video_ids}: {e}")
        return {}

# Store views in database with IST timestamp
def store_views(video_id, views):
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        ist = pytz.timezone("Asia/Kolkata")
        timestamp = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO views (video_id, timestamp, views) VALUES (?, ?, ?)",
                  (video_id, timestamp, views))
        conn.commit()
        logger.debug(f"Stored views for {video_id}: {views} at {timestamp} IST")
    except sqlite3.Error as e:
        logger.error(f"Error storing views for {video_id}: {e}")
    finally:
        conn.close()

# Background task to fetch views at exact 5-minute intervals (e.g., 16:00, 16:05)
def fetch_views_periodically():
    while True:
        try:
            # Get current time in IST
            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            
            # Calculate seconds until the next 5-minute mark
            minutes = now.minute
            seconds = now.second
            minutes_to_next = (5 - (minutes % 5)) % 5
            if minutes_to_next == 0 and seconds == 0:
                seconds_to_wait = 0
            else:
                seconds_to_wait = (minutes_to_next * 60) - seconds
                if seconds_to_wait <= 0:
                    seconds_to_wait += 300  # Wait for the next 5-minute mark
            
            logger.debug(f"Waiting {seconds_to_wait} seconds until the next 5-minute mark")
            time.sleep(seconds_to_wait)
            
            # Fetch the list of videos to track
            conn = sqlite3.connect("views.db", check_same_thread=False)
            c = conn.cursor()
            c.execute("SELECT video_id FROM video_list")
            video_ids = [row[0] for row in c.fetchall()]
            conn.close()
            
            if not video_ids:
                logger.debug("No videos to fetch views for")
                continue
            
            # Fetch and store views
            views_dict = fetch_views(video_ids)
            for video_id, views in views_dict.items():
                if views:
                    store_views(video_id, views)
            
            # Recalculate target metrics for targetable videos
            conn = sqlite3.connect("views.db", check_same_thread=False)
            c = conn.cursor()
            c.execute("SELECT video_id, target_views, target_time FROM targets")
            targets = c.fetchall()
            
            for video_id, target_views, target_time in targets:
                if target_views and target_time:
                    c.execute("SELECT views FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 1", (video_id,))
                    result = c.fetchone()
                    if result:
                        latest_views = result[0]
                        current_time = datetime.now(ist)
                        required_views_per_interval, _ = calculate_required_views_per_interval(
                            latest_views, target_views, target_time, current_time
                        )
                        if required_views_per_interval is not None:
                            c.execute("UPDATE targets SET required_views_per_interval = ? WHERE video_id = ?",
                                      (required_views_per_interval, video_id))
                            conn.commit()
                            logger.debug(f"Updated required views for {video_id}: {required_views_per_interval}")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Background task error: {e}")
        
        # Wait until the next 5-minute mark
        time.sleep(300)  # 5 minutes

# Start background task
def start_background_task():
    thread = threading.Thread(target=fetch_views_periodically, daemon=True)
    thread.start()

# Process data to include view gains
def process_view_gains(data):
    processed_data = []
    for i in range(len(data)):
        timestamp, views = data[i]
        view_gain = 0 if i == 0 else views - data[i-1][1]
        processed_data.append((timestamp, views, view_gain))
    return processed_data

# Calculate required views per 5-minute interval to reach target
def calculate_required_views_per_interval(latest_views, target_views, target_time_str, current_time):
    try:
        target_time = datetime.strptime(target_time_str, "%Y-%m-%d %H:%M:%S")
        target_time = pytz.timezone("Asia/Kolkata").localize(target_time)
        time_diff_seconds = (target_time - current_time).total_seconds()
        if time_diff_seconds <= 0:
            return None, "Target time must be in the future."

        intervals_remaining = time_diff_seconds / 300  # 300 seconds = 5 minutes
        if intervals_remaining < 1:
            return None, "Target time is too close (less than 5 minutes)."

        views_needed = target_views - latest_views
        if views_needed <= 0:
            return None, "Target views already achieved or invalid."

        required_views_per_interval = views_needed / intervals_remaining
        return required_views_per_interval, None
    except ValueError as e:
        return None, f"Invalid target time format: {e}"

# Route for home page
@app.route("/", methods=["GET", "POST"])
def index():
    error_message = None
    videos = []

    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        
        # Fetch the list of videos
        c.execute("SELECT video_id, name, is_targetable FROM video_list ORDER BY video_id = 'hTSaweR8qMI' DESC")
        video_list = c.fetchall()
        
        # Fetch target settings
        c.execute("SELECT video_id, target_views, target_time, required_views_per_interval FROM targets")
        targets = {row[0]: {"target_views": row[1], "target_time": row[2], "required_views_per_interval": row[3]} for row in c.fetchall()}
        
        # Process data for each video
        for video_id, name, is_targetable in video_list:
            c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp ASC", (video_id,))
            video_data = process_view_gains(c.fetchall())
            
            target_info = targets.get(video_id, {"target_views": None, "target_time": None, "required_views_per_interval": None})
            videos.append({
                "video_id": video_id,
                "name": name,
                "data": video_data,
                "is_targetable": is_targetable,
                "target_views": target_info["target_views"],
                "target_time": target_info["target_time"],
                "required_views_per_interval": target_info["required_views_per_interval"],
                "target_message": None
            })
        
        # Handle target views and time submissions
        if request.method == "POST" and "target_views" in request.form:
            video_id = request.form.get("video_id")
            target_views = request.form.get("target_views", type=int)
            target_time = request.form.get("target_time")
            
            if target_views and target_time:
                c.execute("SELECT views FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 1", (video_id,))
                result = c.fetchone()
                if result:
                    latest_views = result[0]
                    current_time = datetime.now(pytz.timezone("Asia/Kolkata"))
                    required_views_per_interval, target_message = calculate_required_views_per_interval(
                        latest_views, target_views, target_time, current_time
                    )
                    
                    # Update or insert target settings
                    c.execute("INSERT OR REPLACE INTO targets (video_id, target_views, target_time, required_views_per_interval) VALUES (?, ?, ?, ?)",
                              (video_id, target_views, target_time, required_views_per_interval))
                    conn.commit()
                    
                    # Update the video's target info in the display list
                    for video in videos:
                        if video["video_id"] == video_id:
                            video["target_views"] = target_views
                            video["target_time"] = target_time
                            video["required_views_per_interval"] = required_views_per_interval
                            video["target_message"] = target_message
                            break
        
        conn.close()

        return render_template(
            "index.html",
            videos=videos,
            error_message=error_message
        )
    
    except sqlite3.Error as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        init_db()
        return render_template(
            "index.html",
            videos=[],
            error_message=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        return render_template(
            "index.html",
            videos=[],
            error_message=str(e)
        )

# Route to add a video
@app.route("/add_video", methods=["POST"])
def add_video():
    try:
        video_id = request.form.get("video_id")
        name = request.form.get("name")
        is_targetable = 1 if request.form.get("is_targetable") == "on" else 0
        
        if not video_id or not name:
            return redirect(url_for("index", error_message="Video ID and Name are required"))
        
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO video_list (video_id, name, is_targetable) VALUES (?, ?, ?)",
                  (video_id, name, is_targetable))
        conn.commit()
        conn.close()
        
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error adding video: {e}")
        return redirect(url_for("index", error_message=str(e)))

# Route to remove a video
@app.route("/remove_video/<video_id>")
def remove_video(video_id):
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        c.execute("DELETE FROM video_list WHERE video_id = ?", (video_id,))
        c.execute("DELETE FROM views WHERE video_id = ?", (video_id,))
        c.execute("DELETE FROM targets WHERE video_id = ?", (video_id,))
        conn.commit()
        conn.close()
        
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error removing video: {e}")
        return redirect(url_for("index", error_message=str(e)))

# Route to export to Excel
@app.route("/export")
def export():
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        
        c.execute("SELECT video_id, name FROM video_list ORDER BY video_id = 'hTSaweR8qMI' DESC")
        video_list = c.fetchall()
        
        dfs = []
        for video_id, name in video_list:
            c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", (video_id,))
            rows = c.fetchall()
            data = [{"Timestamp": row[0], "Views": row[1], "View Gain": 0 if i == 0 else row[1] - rows[i-1][1]} 
                    for i, row in enumerate(rows)]
            df = pd.DataFrame(data)
            dfs.append((name, df))
        
        conn.close()
        
        excel_file = "youtube_views.xlsx"
        with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
            for name, df in dfs:
                df.to_excel(writer, sheet_name=name, index=False)
        
        return send_file(excel_file, as_attachment=True)
    except sqlite3.Error as e:
        logger.error(f"Database error in export route: {e}", exc_info=True)
        return "Database error exporting data", 500
    except Exception as e:
        logger.error(f"Error in export route: {e}", exc_info=True)
        return "Error exporting data", 500

# Initialize database and start background task at app startup
init_db()
start_background_task()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
