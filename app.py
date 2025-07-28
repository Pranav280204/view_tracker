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
            last_three_view_gain_avg REAL DEFAULT 0,
            last_three_like_gain_avg REAL DEFAULT 0,
            last_three_comment_gain_avg REAL DEFAULT 0
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_tracking INTEGER DEFAULT 1
        )""")
        db_conn.commit()
        logger.info("Successfully initialized database")

        c.execute("SELECT COUNT(*) FROM video_list")
        if c.fetchone()[0] == 0:
            default_videos = [
                ("hTSaweR8qMI", "MrBeast", 1),
                ("hxMNYkLN7tI", "Aj Ki Raat", 1),
                ("ekr2nIex040", "Rose", 1)
            ]
            c.executemany("INSERT INTO video_list (video_id, name, is_tracking) VALUES (?, ?, ?)", default_videos)
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

# Fetch views, likes, and comments for multiple video IDs
def fetch_views(video_ids):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return {}
    try:
        response = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        stats = {}
        for item in response.get("items", []):
            video_id = item["id"]
            stats[video_id] = {
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0)),
                "comments": int(item["statistics"].get("commentCount", 0))
            }
        return stats
    except HttpError as e:
        logger.error(f"Error fetching stats for {video_ids}: {e}")
        return {}

# Store views, likes, and comments in database with IST timestamp, date, and last three gain averages
def store_views(video_id, stats):
    try:
        c = db_conn.cursor()
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        date = now.strftime("%Y-%m-%d")
        
        views = stats.get("views", 0)
        likes = stats.get("likes", 0)
        comments = stats.get("comments", 0)
        
        # Fetch the last three records to calculate gains
        c.execute("SELECT views, likes, comments FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 3", (video_id,))
        previous_records = c.fetchall()
        view_gains = []
        like_gains = []
        comment_gains = []
        
        if previous_records and len(previous_records) >= 1:
            # Calculate gains for the current stats against the most recent previous stats
            current_view_gain = views - previous_records[0][0] if previous_records else 0
            current_like_gain = likes - previous_records[0][1] if previous_records else 0
            current_comment_gain = comments - previous_records[0][2] if previous_records else 0
            # Calculate gains for previous records
            for i in range(len(previous_records)-1):
                view_gains.append(previous_records[i][0] - previous_records[i+1][0])
                like_gains.append(previous_records[i][1] - previous_records[i+1][1])
                comment_gains.append(previous_records[i][2] - previous_records[i+1][2])
            view_gains.append(current_view_gain)
            like_gains.append(current_like_gain)
            comment_gains.append(current_comment_gain)
        else:
            view_gains = [0]
            like_gains = [0]
            comment_gains = [0]
        
        # Calculate average of the last three gains
        last_three_view_gain_avg = sum(view_gains[-3:]) / len(view_gains[-3:]) if view_gains else 0
        last_three_like_gain_avg = sum(like_gains[-3:]) / len(like_gains[-3:]) if like_gains else 0
        last_three_comment_gain_avg = sum(comment_gains[-3:]) / len(comment_gains[-3:]) if comment_gains else 0
        
        # Fetch the most recent averages
        c.execute("SELECT last_three_view_gain_avg, last_three_like_gain_avg, last_three_comment_gain_avg FROM views WHERE video_id = ? ORDER BY timestamp DESC LIMIT 1", (video_id,))
        result = c.fetchone()
        previous_view_avg = result[0] if result else 0
        previous_like_avg = result[1] if result else 0
        previous_comment_avg = result[2] if result else 0
        
        # Update only if the new average is greater or no previous average exists
        view_avg_to_store = last_three_view_gain_avg if last_three_view_gain_avg > previous_view_avg or previous_view_avg == 0 else previous_view_avg
        like_avg_to_store = last_three_like_gain_avg if last_three_like_gain_avg > previous_like_avg or previous_like_avg == 0 else previous_like_avg
        comment_avg_to_store = last_three_comment_gain_avg if last_three_comment_gain_avg > previous_comment_avg or previous_comment_avg == 0 else previous_comment_avg
        
        c.execute("INSERT INTO views (video_id, date, timestamp, views, likes, comments, last_three_view_gain_avg, last_three_like_gain_avg, last_three_comment_gain_avg) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                  (video_id, date, timestamp, views, likes, comments, view_avg_to_store, like_avg_to_store, comment_avg_to_store))
        logger.debug(f"Stored stats for {video_id}: views={views}, likes={likes}, comments={comments} at {timestamp} IST, averages: view={view_avg_to_store}, like={like_avg_to_store}, comment={comment_avg_to_store}")
        
        db_conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Error storing stats for {video_id}: {e}")

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

# Background task to fetch stats and add Sourav Joshi video
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
                        # Stop tracking previous Sourav Joshi videos
                        c.execute("""
                            UPDATE video_list 
                            SET is_tracking = 0 
                            WHERE video_id IN (
                                SELECT video_id 
                                FROM video_list 
                                WHERE video_id != ? 
                                AND video_id IN (
                                    SELECT v.video_id 
                                    FROM views v 
                                    JOIN video_list vl ON v.video_id = vl.video_id 
                                    WHERE v.video_id IN (
                                        SELECT video_id 
                                        FROM video_list 
                                        WHERE name LIKE '%Sourav Joshi%'
                                    )
                                )
                            )
                        """, (video_id,))
                        # Add new video
                        c.execute("INSERT OR REPLACE INTO video_list (video_id, name, is_tracking) VALUES (?, ?, ?)",
                                  (video_id, title, 1))
                        db_conn.commit()
                        logger.info(f"Added Sourav Joshi video: {video_id} - {title}, stopped tracking previous videos")
                        stats = fetch_views([video_id])
                        if stats.get(video_id):
                            store_views(video_id, stats[video_id])
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

            # Fetch stats for all actively tracked videos
            c = db_conn.cursor()
            c.execute("SELECT video_id FROM video_list WHERE is_tracking = 1")
            video_ids = [row[0] for row in c.fetchall()]
            if not video_ids:
                logger.debug("No videos to fetch stats for")
                continue

            stats_dict = fetch_views(video_ids)
            for video_id, stats in stats_dict.items():
                if stats:
                    store_views(video_id, stats)
        except Exception as e:
            logger.error(f"Background task error: {e}")

# Start background tasks
def start_background_tasks():
    thread = threading.Thread(target=background_tasks, daemon=True)
    thread.start()

# Process data to include gains and last three gain averages for views, likes, and comments
def process_view_gains(video_id, data):
    processed_data = []
    c = db_conn.cursor()
    for i, (date, timestamp, views, likes, comments, last_three_view_gain_avg, last_three_like_gain_avg, last_three_comment_gain_avg) in enumerate(data):
        view_gain = 0 if i == 0 or data[i-1][0] != date else views - data[i-1][2]
        like_gain = 0 if i == 0 or data[i-1][0] != date else likes - data[i-1][3]
        comment_gain = 0 if i == 0 or data[i-1][0] != date else comments - data[i-1][4]
        
        view_hourly_gain = 0
        like_hourly_gain = 0
        comment_hourly_gain = 0
        timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("""
            SELECT views, likes, comments FROM views 
            WHERE video_id = ? AND date = ? AND timestamp <= ? 
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, date, one_hour_ago))
        result = c.fetchone()
        if result:
            previous_views, previous_likes, previous_comments = result
            view_hourly_gain = views - previous_views
            like_hourly_gain = likes - previous_likes
            comment_hourly_gain = comments - previous_comments
        else:
            logger.debug(f"No hourly gains for {video_id} at {timestamp}: no prior record")
        
        processed_data.append((
            timestamp, views, likes, comments,
            view_gain, like_gain, comment_gain,
            view_hourly_gain, like_hourly_gain, comment_hourly_gain,
            last_three_view_gain_avg, last_three_like_gain_avg, last_three_comment_gain_avg
        ))
    return processed_data

# Route for home page
@app.route("/", methods=["GET"])
def index():
    error_message = None
    videos = []

    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        c.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY video_id = 'hTSaweR8qMI' DESC")
        video_list = c.fetchall()

        for video_id, name, is_tracking in video_list:
            c.execute("SELECT DISTINCT date FROM views WHERE video_id = ? ORDER BY date DESC", (video_id,))
            dates = [row[0] for row in c.fetchall()]
            daily_data = {}
            for date in dates:
                c.execute("SELECT date, timestamp, views, likes, comments, last_three_view_gain_avg, last_three_like_gain_avg, last_three_comment_gain_avg FROM views WHERE video_id = ? AND date = ? ORDER BY timestamp ASC",
                          (video_id, date))
                daily_data[date] = process_view_gains(video_id, c.fetchall())
            
            videos.append({
                "video_id": video_id,
                "name": name,
                "daily_data": daily_data,
                "is_tracking": bool(is_tracking)
            })

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

        stats = fetch_views([video_id])
        if not stats.get(video_id):
            flash("Unable to fetch video data. Check the video link or API key.", "error")
            conn.close()
            return redirect(url_for("index"))

        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO video_list (video_id, name, is_tracking) VALUES (?, ?, ?)",
                  (video_id, title, 1))
        conn.commit()
        conn.close()

        store_views(video_id, stats[video_id])

        flash("Video added successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error adding video: {e}")
        conn.close() if 'conn' in locals() else None
        flash(str(e), "error")
        return redirect(url_for("index"))

# Route to stop tracking a video
@app.route("/stop_tracking/<video_id>")
def stop_tracking(video_id):
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        c.execute("UPDATE video_list SET is_tracking = 0 WHERE video_id = ?", (video_id,))
        conn.commit()
        conn.close()
        flash("Stopped tracking video successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error stopping tracking for video {video_id}: {e}")
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

        c.execute("SELECT date, timestamp, views, likes, comments, last_three_view_gain_avg, last_three_like_gain_avg, last_three_comment_gain_avg FROM views WHERE video_id = ? ORDER BY date, timestamp", (video_id,))
        rows = c.fetchall()
        data = []
        for i, row in enumerate(rows):
            date, timestamp, views, likes, comments, last_three_view_gain_avg, last_three_like_gain_avg, last_three_comment_gain_avg = row
            view_gain = 0 if i == 0 or rows[i-1][0] != date else views - rows[i-1][2]
            like_gain = 0 if i == 0 or rows[i-1][0] != date else likes - rows[i-1][3]
            comment_gain = 0 if i == 0 or rows[i-1][0] != date else comments - rows[i-1][4]
            
            view_hourly_gain = 0
            like_hourly_gain = 0
            comment_hourly_gain = 0
            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            c.execute("""
                SELECT views, likes, comments FROM views 
                WHERE video_id = ? AND date = ? AND timestamp <= ? 
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            result = c.fetchone()
            if result:
                previous_views, previous_likes, previous_comments = result
                view_hourly_gain = views - previous_views
                like_hourly_gain = likes - previous_likes
                comment_hourly_gain = comments - previous_comments
            
            data.append({
                "Date": date,
                "Timestamp": timestamp,
                "Views": views,
                "Likes": likes,
                "Comments": comments,
                "View Gain": view_gain,
                "Like Gain": like_gain,
                "Comment Gain": comment_gain,
                "View Hourly Gain": view_hourly_gain,
                "Like Hourly Gain": like_hourly_gain,
                "Comment Hourly Gain": comment_hourly_gain,
                "Last Three View Gain Avg": last_three_view_gain_avg,
                "Last Three Like Gain Avg": last_three_like_gain_avg,
                "Last Three Comment Gain Avg": last_three_comment_gain_avg
            })
        df = pd.DataFrame(data)

        conn.close()

        excel_file = f"youtube_stats_{video_id}.xlsx"
        df.to_excel(excel_file, sheet_name=name[:31], index=False, engine="openpyxl")

        return send_file(excel_file, as_attachment=True, download_name=f"{name}_stats.xlsx")
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
