import os
import sqlite3
import threading
import time
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, request, send_file
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import logging
import pytz

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
        c.execute("""CREATE TABLE IF NOT EXISTS views (
            video_id TEXT,
            timestamp TEXT,
            views INTEGER
        )""")
        conn.commit()
        logger.info("Database initialized successfully")
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

# Background task to fetch views every 5 minutes
def fetch_views_periodically():
    pathaan_video_id = "YxWlaYCA8MU"  # Jhoome Jo Pathaan
    default_joshi_video_id = "UCR5C2a0pv_5S-0a8pV2y1jg"  # Sourav Joshi default video
    while True:
        try:
            conn = sqlite3.connect("views.db", check_same_thread=False)
            c = conn.cursor()
            c.execute("SELECT DISTINCT video_id FROM views WHERE video_id != ? ORDER BY timestamp DESC LIMIT 1", (pathaan_video_id,))
            result = c.fetchone()
            changeable_video_id = result[0] if result else default_joshi_video_id
            conn.close()

            video_ids = [pathaan_video_id, changeable_video_id]
            views_dict = fetch_views(video_ids)
            
            for video_id, views in views_dict.items():
                if views:
                    store_views(video_id, views)
        except Exception as e:
            logger.error(f"Background task error: {e}")
        time.sleep(300)  # Wait 5 minutes

# Start background task
def start_background_task():
    thread = threading.Thread(target=fetch_views_periodically, daemon=True)
    thread.start()

# Route for home page
@app.route("/", methods=["GET", "POST"])
def index():
    pathaan_video_id = "YxWlaYCA8MU"  # Jhoome Jo Pathaan
    default_joshi_video_id = "UCR5C2a0pv_5S-0a8pV2y1jg"  # Sourav Joshi default video
    error_message = None

    try:
        init_db()
        changeable_video_id = None

        if request.method == "POST":
            changeable_video_id = request.form.get("video_id")
            if changeable_video_id:
                views = fetch_views([changeable_video_id])
                if changeable_video_id in views and views[changeable_video_id]:
                    store_views(changeable_video_id, views[changeable_video_id])
                else:
                    error_message = "Invalid video ID or no view data available"

        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", (pathaan_video_id,))
        pathaan_data = c.fetchall()
        
        if not changeable_video_id:
            c.execute("SELECT DISTINCT video_id FROM views WHERE video_id != ? ORDER BY timestamp DESC LIMIT 1", (pathaan_video_id,))
            result = c.fetchone()
            changeable_video_id = result[0] if result else default_joshi_video_id
        
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", (changeable_video_id,))
        joshi_data = c.fetchall()
        conn.close()

        comparison = []
        ist = pytz.timezone("Asia/Kolkata")
        for i, (pathaan_time, pathaan_views) in enumerate(pathaan_data):
            pathaan_dt = datetime.strptime(pathaan_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ist)
            pathaan_hour = pathaan_dt.replace(minute=0, second=0)
            joshi_views = 0
            for joshi_time, views in joshi_data:
                joshi_dt = datetime.strptime(joshi_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ist)
                joshi_hour = joshi_dt.replace(minute=0, second=0)
                if joshi_hour == pathaan_hour:
                    joshi_views = views
                    break
            comparison.append({
                "hour": pathaan_hour.strftime("%Y-%m-%d %H:00"),
                "pathaan_views": pathaan_views,
                "joshi_views": joshi_views,
                "pathaan_gain": pathaan_views - (pathaan_data[i-1][1] if i > 0 else pathaan_views),
                "joshi_gain": joshi_views - (joshi_data[i-1][1] if i > 0 and joshi_views else joshi_views)
            })

        return render_template("index.html", comparison=comparison, changeable_video_id=changeable_video_id, error_message=error_message)
    
    except sqlite3.Error as e:
        logger.error(f"Database error in index route: {e}", exc_info=True)
        init_db()
        return render_template("index.html", comparison=[], changeable_video_id=changeable_video_id, error_message=f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        return render_template("index.html", comparison=[], changeable_video_id=changeable_video_id, error_message=str(e))

# Route to export data to Excel
@app.route("/export")
def export():
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        
        c.execute("SELECT video_id, timestamp, views FROM views ORDER BY timestamp")
        rows = c.fetchall()
        conn.close()
        
        data = []
        for row in rows:
            video_name = "Jhoome Jo Pathaan" if row[0] == "YxWlaYCA8MU" else "Sourav Joshi (or other)"
            data.append({
                "Video": video_name,
                "Timestamp": row[1],
                "Views": row[2]
            })
        
        df = pd.DataFrame(data)
        excel_file = "youtube_views.xlsx"
        df.to_excel(excel_file, index=False)
        
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
