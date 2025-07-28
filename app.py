import os
import threading
import logging
import pytz
import sqlite3
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
import pandas as pd
from flask import Flask, render_template, send_file, request, redirect, url_for, flash
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

try:
    import psutil
except ImportError:
    psutil = None
    logging.warning("psutil module not found; memory monitoring disabled")

app = Flask(__name__)
app.secret_key = os.urandom(24)  # For flash messages

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# YouTube API setup
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY environment variable is not set")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# SQLite database connection (single connection for background task)
db_conn = None

def init_db():
    global db_conn
    try:
        db_conn = sqlite3.connect("views.db", check_same_thread=False)
        c = db_conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS views (
            video_id TEXT,
            date TEXT,
            timestamp TEXT,
            views INTEGER,
            likes INTEGER,
            comments INTEGER,
            last_three_gain_avg REAL DEFAULT 0
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_targetable INTEGER,
            is_tracking INTEGER DEFAULT 1
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS targets (
            video_id TEXT PRIMARY KEY,
            target_views INTEGER,
            target_time TEXT,
            required_views_per_interval REAL
        )""")
        db_conn.commit()
        logger.info("Successfully initialized database")

        c.execute("SELECT COUNT(*) FROM video_list")
        if c.fetchone()[0] == 0:
            default_videos = [
                ("hTSaweR8qMI", "MrBeast", 1, 1),
                ("hxMNYkLN7tI", "Aj Ki Raat", 0, 1),
                ("ekr2nIex040", "Rose", 0, 1)
            ]
            c.executemany("INSERT INTO video_list (video_id, name, is_targetable, is_tracking) VALUES (?, ?, ?, ?)", default_videos)
            db_conn.commit()
            logger.info("Initialized default videos")
    except sqlite3.Error as e:
        logger.error(f"Database initialization failed: {e}")
    # Do not close db_conn here to reuse it

# Fetch views, likes, and comments for multiple video IDs
def fetch_video_stats(video_ids):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return {}
    try:
        response = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        video_stats = {}
        for item in response.get("items", []):
            video_id = item["id"]
            views = int(item["statistics"]["viewCount"])
            likes = int(item["statistics"].get("likeCount", 0))  # Some videos might not have like count
            comments = int(item["statistics"].get("commentCount", 0))  # Some videos might not have comment count
            video_stats[video_id] = {"views": views, "likes": likes, "comments": comments}
        return video_stats
    except HttpError as e:
        logger.error(f"Error fetching stats for {video_ids}: {e}")
        return {}

# Store views, likes, and comments in the database
def store_views(video_id, views, likes, comments):
    try:
        c = db_conn.cursor()
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        date = now.strftime("%Y-%m-%d")
        
        # Fetch the last three view counts to calculate view gains
        c.execute("SELECT views FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 3", (video_id,))
        previous_views = [row[0] for row in c.fetchall()]
        view_gains = []
        if previous_views and len(previous_views) >= 1:
            # Calculate view gain for the current views against the most recent previous views
            current_gain = views - previous_views[0] if previous_views else 0
            # Calculate view gains for previous views
            for i in range(len(previous_views)-1):
                gain = previous_views[i] - previous_views[i+1]
                view_gains.append(gain)
            view_gains.append(current_gain)
        else:
            view_gains = [0]  # No previous views, so gain is 0 for the first entry
        
        # Calculate average of the last three view gains (or fewer if not enough data)
        last_three_gain_avg = sum(view_gains[-3:]) / len(view_gains[-3:]) if view_gains else 0
        
        # Fetch the most recent last_three_gain_avg
        c.execute("SELECT last_three_gain_avg FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 1", (video_id,))
        result = c.fetchone()
        previous_avg = result[0] if result else 0
        
        # Only update if the new average is greater than the previous or if no previous average exists
        if last_three_gain_avg > previous_avg or previous_avg == 0:
            c.execute("INSERT INTO views (video_id, date, timestamp, views, likes, comments, last_three_gain_avg) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (video_id, date, timestamp, views, likes, comments, last_three_gain_avg))
            logger.debug(f"Stored views for {video_id}: {views} views, {likes} likes, {comments} comments at {timestamp} IST, last_three_gain_avg: {last_three_gain_avg}")
        else:
            c.execute("INSERT INTO views (video_id, date, timestamp, views, likes, comments, last_three_gain_avg) VALUES (?, ?, ?, ?, ?, ?, ?)",
                      (video_id, date, timestamp, views, likes, comments, previous_avg))
            logger.debug(f"Stored views for {video_id}: {views} views, {likes} likes, {comments} comments at {timestamp} IST, retained last_three_gain_avg: {previous_avg}")
        
        db_conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error storing views for {video_id}: {e}")

# Background task to fetch views, likes, and comments for all videos
def background_tasks():
    global db_conn
    while True:
        try:
            # Fetch views, likes, and comments for all actively tracked videos
            c = db_conn.cursor()
            c.execute("SELECT video_id FROM video_list WHERE is_tracking = 1")
            video_ids = [row[0] for row in c.fetchall()]
            if not video_ids:
                logger.debug("No videos to fetch stats for")
                continue

            stats_dict = fetch_video_stats(video_ids)
            for video_id, stats in stats_dict.items():
                if stats:
                    store_views(video_id, stats["views"], stats["likes"], stats["comments"])

            time.sleep(300)  # Wait 5 minutes before fetching again

        except Exception as e:
            logger.error(f"Background task error: {e}")

# Initialize background tasks
def start_background_tasks():
    thread = threading.Thread(target=background_tasks, daemon=True)
    thread.start()

# Route for home page
@app.route("/", methods=["GET", "POST"])
def index():
    error_message = None
    videos = []

    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        c.execute("SELECT video_id, name, is_targetable, is_tracking FROM video_list ORDER BY video_id = 'hTSaweR8qMI' DESC")
        video_list = c.fetchall()

        for video_id, name, is_targetable, is_tracking in video_list:
            c.execute("SELECT DISTINCT date FROM views WHERE video_id = ? ORDER BY date DESC", (video_id,))
            dates = [row[0] for row in c.fetchall()]
            daily_data = {}
            for date in dates:
                c.execute("SELECT date, timestamp, views, likes, comments, last_three_gain_avg FROM views WHERE video_id = ? AND date = ? ORDER BY timestamp ASC",
                          (video_id, date))
                daily_data[date] = c.fetchall()
            
            videos.append({
                "video_id": video_id,
                "name": name,
                "daily_data": daily_data,
                "is_targetable": bool(is_targetable),
                "is_tracking": bool(is_tracking),
            })

        conn.close()
        return render_template("index.html", videos=videos, error_message=error_message)
    except sqlite3.Error as e:
        logger.error(f"Error in index route: {e}")
        conn.close() if 'conn' in locals() else None
        init_db()
        return render_template("index.html", videos=[], error_message=f"Database error: {e}")
    except Exception as e:
        logger.error(f"Error in index route: {e}")
        conn.close() if 'conn' in locals() else None
        return render_template("index.html", videos=[], error_message=str(e))

# Export views data to Excel for a specific video
@app.route("/export/<video_id>")
def export(video_id):
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        c.execute("SELECT name FROM video_list WHERE video_id = ?", (video_id,))
        result = c.fetchone()
        if not result:
            flash("Video not found.", "error")
            conn.close()
            return redirect(url_for("index"))
        name = result[0]

        c.execute("SELECT date, timestamp, views, likes, comments, last_three_gain_avg FROM views WHERE video_id = ? ORDER BY date, timestamp", (video_id,))
        rows = c.fetchall()
        data = []
        for row in rows:
            date, timestamp, views, likes, comments, last_three_gain_avg = row
            data.append({
                "Date": date,
                "Timestamp": timestamp,
                "Views": views,
                "Likes": likes,
                "Comments": comments,
                "Last Three Gain Avg": last_three_gain_avg
            })
        df = pd.DataFrame(data)

        conn.close()

        excel_file = f"youtube_views_{video_id}.xlsx"
        df.to_excel(excel_file, sheet_name=name[:31], index=False, engine="openpyxl")

        return send_file(excel_file, as_attachment=True, download_name=f"{name}_views.xlsx")
    except sqlite3.Error as e:
        logger.error(f"Database error in export route: {e}", exc_info=True)
        conn.close() if 'conn' in locals() else None
        flash("Database error exporting data.", "error")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error in export route: {e}", exc_info=True)
        conn.close() if 'conn' in locals() else None
        flash("Error exporting data.", "error")
        return redirect(url_for("index"))

# Initialize database and start background tasks
init_db()
start_background_tasks()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
