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
            views INTEGER
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_targetable INTEGER
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
                ("hTSaweR8qMI", "MrBeast", 1),
                ("hxMNYkLN7tI", "Aj Ki Raat", 0),
                ("ekr2nIex040", "Rose", 0)
            ]
            c.executemany("INSERT INTO video_list (video_id, name, is_targetable) VALUES (?, ?, ?)", default_videos)
            db_conn.commit()
            logger.info("Initialized default videos")
    except sqlite3.Error as e:
        logger.error(f"Database initialization failed: {e}")
    # Do not close db_conn here to reuse it

# Extract video_id from YouTube URL
def extract_video_id(video_link):
    try:
        parsed_url = urlparse(video_link)
        if parsed_url.hostname in ("www.youtube.com", "youtube.com", "youtu.be"):
            if parsed_url.hostname == "youtu.be":
                return parsed_url.path[1:] if len(parsed_url.path) > 1 else None
            query = parse_qs(parsed_url.query)
            return query.get("v", [None])[0]
        return None
    except Exception as e:
        logger.error(f"Error parsing video link: {e}")
        return None

# Fetch video title from YouTube API
def fetch_video_title(video_id):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return None
    try:
        response = youtube.videos().list(part="snippet", id=video_id).execute()
        for item in response.get("items", []):
            return item["snippet"]["title"][:50]  # Truncate to 50 characters
        return None
    except HttpError as e:
        logger.error(f"Error fetching title for {video_id}: {e}")
        return None

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

# Store views in database with IST timestamp and date
def store_views(video_id, views):
    try:
        c = db_conn.cursor()
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        date = now.strftime("%Y-%m-%d")
        c.execute("INSERT INTO views (video_id, date, timestamp, views) VALUES (?, ?, ?, ?)",
                  (video_id, date, timestamp, views))
        db_conn.commit()
        logger.debug(f"Stored views for {video_id}: {views} at {timestamp} IST")
    except sqlite3.Error as e:
        logger.error(f"Error storing views for {video_id}: {e}")

# Fetch latest video from Sourav Joshi Vlogs channel
def fetch_latest_sourav_joshi_video():
    if not youtube:
        logger.error("YouTube API client not initialized")
        return None, None, None
    try:
        response = youtube.search().list(
            part="id,snippet",
            channelId="UCj0Cw6g1v3q4ruQVWdQe2zw",  # Sourav Joshi Vlogs channel ID
            maxResults=1,
            order="date",
            type="video"
        ).execute()
        for item in response.get("items", []):
            video_id = item["id"]["videoId"]
            title = item["snippet"]["title"][:50]  # Truncate title
            published_at = datetime.strptime(item["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
            published_at = pytz.utc.localize(published_at).astimezone(pytz.timezone("Asia/Kolkata"))
            return video_id, title, published_at
        return None, None, None
    except HttpError as e:
        logger.error(f"Error fetching latest Sourav Joshi video: {e}")
        return None, None, None

# Background task to fetch views and add Sourav Joshi video
def background_tasks():
    global db_conn
    last_sourav_check = None
    while True:
        try:
            if psutil:
                process = psutil.Process()
                memory_info = process.memory_info()
                logger.debug(f"Memory usage: RSS={memory_info.rss / 1024 / 1024:.2f} MB, VMS={memory_info.vms / 1024 / 1024:.2f} MB")
            else:
                logger.debug("Memory monitoring skipped: psutil not available")

            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            current_time = now.time()
            current_date = now.date()

            # Check for new Sourav Joshi video at 8:05 AM IST
            if current_time.hour == 8 and current_time.minute == 5 and (last_sourav_check is None or last_sourav_check.date() != current_date):
                video_id, title, published_at = fetch_latest_sourav_joshi_video()
                if video_id and published_at.date() == current_date:
                    c = db_conn.cursor()
                    c.execute("SELECT video_id FROM video_list WHERE video_id = ?", (video_id,))
                    if not c.fetchone():
                        c.execute("INSERT OR REPLACE INTO video_list (video_id, name, is_targetable) VALUES (?, ?, ?)",
                                  (video_id, title, 1))
                        db_conn.commit()
                        logger.info(f"Added Sourav Joshi video: {video_id} - {title}")
                        views = fetch_views([video_id])
                        if views.get(video_id):
                            store_views(video_id, views[video_id])
                last_sourav_check = now

            # Calculate time until next 5-minute mark
            current_minutes = now.minute
            current_seconds = now.second + (now.microsecond / 1_000_000)  # Include microseconds for precision
            minutes_to_next = 5 - (current_minutes % 5)
            if minutes_to_next == 5:
                minutes_to_next = 0  # At 5-minute mark, wait full 5 minutes
            seconds_to_wait = (minutes_to_next * 60) - current_seconds
            if seconds_to_wait <= 0:
                seconds_to_wait += 300  # If at or past the mark, wait 5 minutes
            logger.debug(f"Current time: {now}, minutes_to_next: {minutes_to_next}, seconds_to_wait: {seconds_to_wait:.2f}")
            time.sleep(seconds_to_wait)

            # Fetch views for all videos
            c = db_conn.cursor()
            c.execute("SELECT video_id FROM video_list")
            video_ids = [row[0] for row in c.fetchall()]
            if not video_ids:
                logger.debug("No videos to fetch views for")
                continue

            views_dict = fetch_views(video_ids)
            for video_id, views in views_dict.items():
                if views:
                    store_views(video_id, views)

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
                            db_conn.commit()
                            logger.debug(f"Updated required views for {video_id}: {required_views_per_interval}")
        except Exception as e:
            logger.error(f"Background task error: {e}")

# Start background tasks
def start_background_tasks():
    thread = threading.Thread(target=background_tasks, daemon=True)
    thread.start()

# Process data to include view gains and hourly gains
def process_view_gains(video_id, data):
    processed_data = []
    c = db_conn.cursor()
    for i, (date, timestamp, views) in enumerate(data):
        view_gain = 0 if i == 0 or data[i-1][0] != date else views - data[i-1][2]
        hourly_gain = 0  # Default to 0 instead of None
        timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""
            SELECT views FROM views 
            WHERE video_id = ? AND date = ? AND timestamp <= ? 
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, date, one_hour_ago))
        result = c.fetchone()
        if result:
            previous_views = result[0]
            hourly_gain = views - previous_views
        else:
            logger.debug(f"No hourly gain for {video_id} at {timestamp}: no prior record")
        processed_data.append((timestamp, views, view_gain, hourly_gain))
    return processed_data

# Calculate required views per 5-minute interval
def calculate_required_views_per_interval(latest_views, target_views, target_time_str, current_time):
    try:
        if target_views is None or latest_views is None:
            return None, "Invalid view counts provided."
        target_time = datetime.strptime(target_time_str, "%Y-%m-%dT%H:%M")
        target_time = pytz.timezone("Asia/Kolkata").localize(target_time)
        time_diff_seconds = (target_time - current_time).total_seconds()
        if time_diff_seconds <= 0:
            return None, "Target time must be in the future."
        intervals_remaining = time_diff_seconds / 300
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
        c.execute("SELECT video_id, name, is_targetable FROM video_list ORDER BY video_id = 'hTSaweR8qMI' DESC")
        video_list = c.fetchall()
        c.execute("SELECT video_id, target_views, target_time, required_views_per_interval FROM targets")
        targets = {row[0]: {"target_views": row[1], "target_time": row[2], "required_views_per_interval": row[3]} for row in c.fetchall()}

        for video_id, name, is_targetable in video_list:
            c.execute("SELECT DISTINCT date FROM views WHERE video_id = ? ORDER BY date DESC", (video_id,))
            dates = [row[0] for row in c.fetchall()]
            daily_data = {}
            for date in dates:
                c.execute("SELECT date, timestamp, views FROM views WHERE video_id = ? AND date = ? ORDER BY timestamp ASC",
                          (video_id, date))
                daily_data[date] = process_view_gains(video_id, c.fetchall())
            
            target_info = targets.get(video_id, {"target_views": None, "target_time": None, "required_views_per_interval": None})
            videos.append({
                "video_id": video_id,
                "name": name,
                "daily_data": daily_data,
                "is_targetable": bool(is_targetable),
                "target_views": target_info["target_views"],
                "target_time": target_info["target_time"],
                "required_views_per_interval": target_info["required_views_per_interval"],
                "target_message": None
            })

        if request.method == "POST" and "target_views" in request.form:
            video_id = request.form.get("video_id")
            target_views = request.form.get("target_views")
            target_time = request.form.get("target_time")
            logger.debug(f"Received target form: video_id={video_id}, target_views={target_views}, target_time={target_time}")
            try:
                target_views = int(target_views) if target_views else None
                if not target_views or target_views <= 0:
                    flash("Target views must be a positive number.", "error")
                elif not target_time:
                    flash("Target time is required.", "error")
                else:
                    c.execute("SELECT views FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 1", (video_id,))
                    result = c.fetchone()
                    if result:
                        latest_views = result[0]
                        current_time = datetime.now(pytz.timezone("Asia/Kolkata"))
                        required_views_per_interval, target_message = calculate_required_views_per_interval(
                            latest_views, target_views, target_time, current_time
                        )
                        if target_message:
                            flash(target_message, "error")
                        else:
                            c.execute("INSERT OR REPLACE INTO targets (video_id, target_views, target_time, required_views_per_interval) VALUES (?, ?, ?, ?)",
                                      (video_id, target_views, target_time, required_views_per_interval))
                            conn.commit()
                            for video in videos:
                                if video["video_id"] == video_id:
                                    video["target_views"] = target_views
                                    video["target_time"] = target_time
                                    video["required_views_per_interval"] = required_views_per_interval
                                    video["target_message"] = target_message
                                    break
                            flash("Target set successfully.", "success")
                    else:
                        flash("No view data available for this video.", "error")
            except ValueError:
                flash("Target views must be a valid number.", "error")
                logger.error(f"Invalid target_views value: {target_views}")

        conn.close()

        return render_template(
            "index.html",
            videos=videos,
            error_message=error_message
        )

    except sqlite3.Error as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        conn.close() if 'conn' in locals() else None
        init_db()
        return render_template(
            "index.html",
            videos=[],
            error_message=f"Database error: {e}"
        )
    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        conn.close() if 'conn' in locals() else None
        return render_template(
            "index.html",
            videos=[],
            error_message=str(e)
        )

# Route to add a video
@app.route("/add_video", methods=["POST"])
def add_video():
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        video_link = request.form.get("video_link")
        is_targetable = 1 if request.form.get("is_targetable") == "on" else 0

        if not video_link:
            flash("Video link is required.", "error")
            conn.close()
            return redirect(url_for("index"))

        video_id = extract_video_id(video_link)
        if not video_id:
            flash("Invalid YouTube video link.", "error")
            conn.close()
            return redirect(url_for("index"))

        title = fetch_video_title(video_id)
        if not title:
            flash("Unable to fetch video title. Check the video link or API key.", "error")
            conn.close()
            return redirect(url_for("index"))

        views = fetch_views([video_id])
        if not views.get(video_id):
            flash("Unable to fetch video data. Check the video link or API key.", "error")
            conn.close()
            return redirect(url_for("index"))

        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO video_list (video_id, name, is_targetable) VALUES (?, ?, ?)",
                  (video_id, title, is_targetable))
        conn.commit()
        conn.close()

        store_views(video_id, views[video_id])

        flash("Video added successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error adding video: {e}")
        conn.close() if 'conn' in locals() else None
        flash(str(e), "error")
        return redirect(url_for("index"))

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
        flash("Video removed successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error removing video: {e}")
        conn.close() if 'conn' in locals() else None
        flash(str(e), "error")
        return redirect(url_for("index"))

# Route to export to Excel for a specific video
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

        c.execute("SELECT date, timestamp, views FROM views WHERE video_id = ? ORDER BY date, timestamp", (video_id,))
        rows = c.fetchall()
        data = []
        for i, row in enumerate(rows):
            date, timestamp, views = row
            view_gain = 0 if i == 0 or rows[i-1][0] != date else views - rows[i-1][2]
            hourly_gain = 0
            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            c.execute("""
                SELECT views FROM views 
                WHERE video_id = ? AND date = ? AND timestamp <= ? 
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            result = c.fetchone()
            if result:
                previous_views = result[0]
                hourly_gain = views - previous_views
            data.append({
                "Date": date,
                "Timestamp": timestamp,
                "Views": views,
                "View Gain": view_gain,
                "Hourly Gain": hourly_gain
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
