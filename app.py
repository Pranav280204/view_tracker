import os
import threading
import logging
import pytz
import psycopg2
import psycopg2.extras
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

# Postgres database connection (single connection for background task)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    # fallback if you want to hardcode locally; remove/hide in production
    None
)
if not DATABASE_URL:
    logger.error("DATABASE_URL environment variable not set. Set it to your Postgres connection URL.")
db_conn = None


def get_new_conn():
    """Return a new psycopg2 connection (use for short-lived operations)."""
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    """Initialize postgres tables if they do not exist and set global db_conn for background task."""
    global db_conn
    try:
        # open persistent connection for background thread usage
        db_conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        c = db_conn.cursor()
        # Create tables (similar schema as sqlite)
        c.execute("""
            CREATE TABLE IF NOT EXISTS video_list (
                video_id TEXT PRIMARY KEY,
                name TEXT,
                is_tracking BOOLEAN DEFAULT TRUE,
                comparison_video_id TEXT
            );
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS views (
                id SERIAL PRIMARY KEY,
                video_id TEXT,
                date TEXT,
                timestamp TEXT,
                views INTEGER,
                likes INTEGER
            );
        """)
        db_conn.commit()
        logger.info("Successfully initialized Postgres database")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")


# Extract video_id from YouTube URL
def extract_video_id(video_link):
    try:
        parsed_url = urlparse(video_link)
        hostname = parsed_url.hostname or ""
        hostname = hostname.lower()
        if "youtu.be" in hostname:
            return parsed_url.path[1:] if len(parsed_url.path) > 1 else None
        if "youtube.com" in hostname:
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
    except Exception as e:
        logger.error(f"Unexpected error fetching title for {video_id}: {e}")
        return None


# Fetch views and likes for multiple video IDs
def fetch_views(video_ids):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return {}
    try:
        if not video_ids:
            return {}
        response = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        stats = {}
        for item in response.get("items", []):
            video_id = item["id"]
            stats[video_id] = {
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0))
            }
        return stats
    except HttpError as e:
        logger.error(f"Error fetching stats for {video_ids}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error fetching stats for {video_ids}: {e}")
        return {}


# Store views and likes in database with IST timestamp and date
def store_views(video_id, stats):
    try:
        c = db_conn.cursor()
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        date = now.strftime("%Y-%m-%d")

        views = stats.get("views", 0)
        likes = stats.get("likes", 0)

        c.execute(
            "INSERT INTO views (video_id, date, timestamp, views, likes) VALUES (%s, %s, %s, %s, %s)",
            (video_id, date, timestamp, views, likes)
        )
        logger.debug(f"Stored stats for {video_id}: views={views}, likes={likes} at {timestamp} IST")

        db_conn.commit()
    except Exception as e:
        logger.error(f"Error storing stats for {video_id}: {e}")
        # try to recover broken connection
        try:
            db_conn.rollback()
        except Exception:
            pass


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
    except Exception as e:
        logger.error(f"Unexpected error fetching latest Sourav Joshi video: {e}")
        return None, None, None


# Background task to fetch stats and add Sourav Joshi video
def background_tasks():
    global db_conn
    last_sourav_check = None
    default_comparison_id = "YxWlaYCA8MU"  # Default comparison video ID

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
                    c.execute("SELECT video_id FROM video_list WHERE video_id = %s", (video_id,))
                    if not c.fetchone():
                        # Stop tracking previous Sourav Joshi videos
                        c.execute("""
                            UPDATE video_list 
                            SET is_tracking = FALSE 
                            WHERE video_id IN (
                                SELECT video_id 
                                FROM video_list 
                                WHERE video_id != %s
                                AND video_id IN (
                                    SELECT v.video_id
                                    FROM views v
                                    JOIN video_list vl ON v.video_id = vl.video_id
                                    WHERE v.video_id IN (
                                        SELECT video_id FROM video_list WHERE name LIKE '%%Sourav Joshi%%'
                                    )
                                )
                            )
                        """, (video_id,))
                        # Add new video with default comparison
                        c.execute("""
                            INSERT INTO video_list (video_id, name, is_tracking, comparison_video_id)
                            VALUES (%s, %s, TRUE, %s)
                            ON CONFLICT (video_id) DO UPDATE
                            SET name = EXCLUDED.name, is_tracking = TRUE, comparison_video_id = EXCLUDED.comparison_video_id
                        """, (video_id, title, default_comparison_id))
                        db_conn.commit()
                        logger.info(f"Added Sourav Joshi video: {video_id} - {title} with default comparison {default_comparison_id}, stopped tracking previous videos")
                        stats = fetch_views([video_id, default_comparison_id])
                        if stats.get(video_id):
                            store_views(video_id, stats[video_id])
                        if stats.get(default_comparison_id):
                            store_views(default_comparison_id, stats[default_comparison_id])
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

            # Fetch stats for all actively tracked videos and their comparisons
            c = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            c.execute("SELECT video_id, comparison_video_id FROM video_list WHERE is_tracking = TRUE")
            video_pairs = c.fetchall()
            video_ids = [pair['video_id'] for pair in video_pairs]
            comparison_ids = [pair['comparison_video_id'] for pair in video_pairs if pair['comparison_video_id']]
            all_ids = list(set([vid for vid in (video_ids + comparison_ids) if vid]))
            if not all_ids:
                logger.debug("No videos to fetch stats for")
                continue

            stats_dict = fetch_views(all_ids)
            for video_id in video_ids:
                if stats_dict.get(video_id):
                    store_views(video_id, stats_dict[video_id])
            for comp_id in comparison_ids:
                if stats_dict.get(comp_id):
                    store_views(comp_id, stats_dict[comp_id])
        except Exception as e:
            logger.error(f"Background task error: {e}")
            # if db_conn is broken, attempt to recreate it
            try:
                if db_conn:
                    db_conn.close()
            except Exception:
                pass
            try:
                time.sleep(10)
                # reinit connection
                init_db()
            except Exception:
                pass
            time.sleep(60)


# Start background tasks
def start_background_tasks():
    global db_conn
    thread = threading.Thread(target=background_tasks, daemon=True)
    thread.start()


# Process data to include gains for views, likes, views/likes ratio, and comparison view ratio
def process_view_gains(video_id, data_rows, comparison_video_id=None):
    """
    data_rows: list of tuples (date, timestamp, views, likes) OR list of psycopg2 row objects
    returns: list of tuples (timestamp, views, likes, view_gain, like_gain, view_hourly_gain, view_like_ratio, like_hourly_gain, comp_view_ratio)
    """
    processed_data = []
    # use a fresh short-lived connection for queries inside this function to avoid conflicts with background db_conn
    conn = get_new_conn()
    c = conn.cursor()
    # Accept either tuple rows or dict-like rows
    for i, row in enumerate(data_rows):
        # support both tuple (date, timestamp, views, likes) and dict/psycopg2 row
        if isinstance(row, (list, tuple)):
            date, timestamp, views, likes = row
        else:
            # psycopg2 extras DictRow
            date, timestamp, views, likes = row['date'], row['timestamp'], row['views'], row['likes']

        view_gain = 0
        like_gain = 0
        if i != 0 and (isinstance(data_rows[i-1], (list, tuple)) and data_rows[i-1][0] == date or (not isinstance(data_rows[i-1], (list, tuple)) and data_rows[i-1]['date'] == date)):
            prev = data_rows[i-1]
            prev_views = prev[2] if isinstance(prev, (list, tuple)) else prev['views']
            prev_likes = prev[3] if isinstance(prev, (list, tuple)) else prev['likes']
            view_gain = views - prev_views
            like_gain = likes - prev_likes

        view_like_ratio = round(views / likes, 2) if likes > 0 else 0

        view_hourly_gain = 0
        like_hourly_gain = 0
        comp_view_ratio = None
        timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")  # 60-minute window

        # Hourly view gain (60-minute interval)
        c.execute("""
            SELECT views FROM views 
            WHERE video_id = %s AND date = %s AND timestamp <= %s 
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, date, one_hour_ago))
        result = c.fetchone()
        if result:
            previous_views = result[0]
            view_hourly_gain = views - previous_views
        else:
            logger.debug(f"No 60-minute view gain for {video_id} at {timestamp}: no prior record")

        # Hourly like gain (60-minute interval)
        c.execute("""
            SELECT likes FROM views 
            WHERE video_id = %s AND date = %s AND timestamp <= %s 
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, date, one_hour_ago))
        result = c.fetchone()
        if result:
            previous_likes = result[0]
            like_hourly_gain = likes - previous_likes
        else:
            logger.debug(f"No 60-minute like gain for {video_id} at {timestamp}: no prior record")

        # Comparison view ratio (primary video's 60-minute gain / comparison video's 60-minute gain)
        if comparison_video_id:
            c.execute("""
                SELECT views FROM views 
                WHERE video_id = %s AND date = %s AND timestamp <= %s 
                ORDER BY timestamp DESC LIMIT 1
            """, (comparison_video_id, date, one_hour_ago))
            comp_result = c.fetchone()
            if comp_result:
                comp_previous_views = comp_result[0]
                c.execute("""
                    SELECT views FROM views 
                    WHERE video_id = %s AND date = %s AND timestamp = %s
                """, (comparison_video_id, date, timestamp))
                comp_current_result = c.fetchone()
                if comp_current_result and comp_current_result[0] > 0:
                    comp_view_hourly_gain = comp_current_result[0] - comp_previous_views
                    if comp_view_hourly_gain != 0:  # Avoid division by zero
                        comp_view_ratio = round(view_hourly_gain / comp_view_hourly_gain, 2)
                    else:
                        comp_view_ratio = None
                        logger.debug(f"Comparison view gain is zero for {comparison_video_id} at {timestamp}")
                else:
                    comp_view_ratio = None
                    logger.debug(f"No current views for {comparison_video_id} at {timestamp}")
            else:
                comp_view_ratio = None
                logger.debug(f"No previous views for {comparison_video_id} at {timestamp}")

        processed_data.append((
            timestamp, views, likes, view_gain, like_gain, view_hourly_gain, view_like_ratio, like_hourly_gain, comp_view_ratio
        ))
    conn.close()
    return processed_data


# Route for home page
@app.route("/", methods=["GET"])
def index():
    error_message = None
    videos = []

    try:
        # Use short-lived connection for web request
        conn = get_new_conn()
        c = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        c.execute("SELECT video_id, name, is_tracking, comparison_video_id FROM video_list ORDER BY name ASC")
        video_list = c.fetchall()

        for row in video_list:
            video_id = row['video_id']
            name = row['name']
            is_tracking = row['is_tracking']
            comparison_video_id = row['comparison_video_id']

            c.execute("SELECT DISTINCT date FROM views WHERE video_id = %s ORDER BY date DESC", (video_id,))
            dates = [r[0] for r in c.fetchall()]
            daily_data = {}
            for date in dates:
                c.execute("SELECT date, timestamp, views, likes FROM views WHERE video_id = %s AND date = %s ORDER BY timestamp ASC",
                          (video_id, date))
                rows = c.fetchall()
                # rows will be list of tuples; pass them to process_view_gains
                daily_data[date] = process_view_gains(video_id, rows, comparison_video_id)

            videos.append({
                "video_id": video_id,
                "name": name,
                "daily_data": daily_data,
                "is_tracking": bool(is_tracking),
                "comparison_video_id": comparison_video_id
            })

        conn.close()

        return render_template(
            "index.html",
            videos=videos,
            error_message=error_message
        )

    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        try:
            conn.close()
        except Exception:
            pass
        # If DB error, attempt to initialize
        init_db()
        return render_template(
            "index.html",
            videos=[],
            error_message=f"Database error: {e}"
        )


# Route to add a video
@app.route("/add_video", methods=["POST"])
def add_video():
    try:
        video_link = request.form.get("video_link")
        comparison_link = request.form.get("comparison_link")

        if not video_link:
            flash("Video link is required.", "error")
            return redirect(url_for("index"))

        video_id = extract_video_id(video_link)
        if not video_id:
            flash("Invalid YouTube video link.", "error")
            return redirect(url_for("index"))

        title = fetch_video_title(video_id)
        if not title:
            flash("Unable to fetch video title. Check the video link or API key.", "error")
            return redirect(url_for("index"))

        comparison_video_id = None
        if comparison_link:
            comparison_video_id = extract_video_id(comparison_link)
            if not comparison_video_id:
                flash("Invalid comparison YouTube video link.", "error")
                return redirect(url_for("index"))
            comp_title = fetch_video_title(comparison_video_id)
            if not comp_title:
                flash("Unable to fetch comparison video title. Check the link or API key.", "error")
                return redirect(url_for("index"))

        stats = fetch_views([video_id] + ([comparison_video_id] if comparison_video_id else []))
        if not stats.get(video_id):
            flash("Unable to fetch video data. Check the video link or API key.", "error")
            return redirect(url_for("index"))
        if comparison_video_id and not stats.get(comparison_video_id):
            flash("Unable to fetch comparison video data. Check the link or API key.", "error")
            return redirect(url_for("index"))

        conn = get_new_conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO video_list (video_id, name, is_tracking, comparison_video_id)
            VALUES (%s, %s, TRUE, %s)
            ON CONFLICT (video_id) DO UPDATE
            SET name = EXCLUDED.name, is_tracking = TRUE, comparison_video_id = EXCLUDED.comparison_video_id
        """, (video_id, title, comparison_video_id))
        conn.commit()
        conn.close()

        # store immediate stats
        store_views(video_id, stats[video_id])
        if comparison_video_id:
            store_views(comparison_video_id, stats[comparison_video_id])

        flash("Video added successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error adding video: {e}", exc_info=True)
        flash(str(e), "error")
        return redirect(url_for("index"))


# Route to stop tracking a video
@app.route("/stop_tracking/<video_id>")
def stop_tracking(video_id):
    try:
        conn = get_new_conn()
        c = conn.cursor()
        c.execute("UPDATE video_list SET is_tracking = FALSE WHERE video_id = %s", (video_id,))
        conn.commit()
        conn.close()
        flash("Stopped tracking video successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error stopping tracking for video {video_id}: {e}", exc_info=True)
        flash(str(e), "error")
        return redirect(url_for("index"))


# Route to remove a video
@app.route("/remove_video/<video_id>")
def remove_video(video_id):
    try:
        conn = get_new_conn()
        c = conn.cursor()
        # remove video_list entry and its views (views has no fk cascade in case, so delete views explicitly)
        c.execute("SELECT comparison_video_id FROM video_list WHERE video_id = %s", (video_id,))
        result = c.fetchone()
        comparison_video_id = result[0] if result else None
        c.execute("DELETE FROM video_list WHERE video_id = %s", (video_id,))
        c.execute("DELETE FROM views WHERE video_id = %s", (video_id,))
        if comparison_video_id:
            c.execute("DELETE FROM views WHERE video_id = %s", (comparison_video_id,))
        conn.commit()
        conn.close()
        flash("Video removed successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error removing video: {e}", exc_info=True)
        flash(str(e), "error")
        return redirect(url_for("index"))


# Route to export to Excel for a specific video
@app.route("/export/<video_id>")
def export(video_id):
    try:
        conn = get_new_conn()
        c = conn.cursor()
        c.execute("SELECT name, comparison_video_id FROM video_list WHERE video_id = %s", (video_id,))
        result = c.fetchone()
        if not result:
            flash("Video not found.", "error")
            conn.close()
            return redirect(url_for("index"))
        name, comparison_video_id = result[0], result[1]

        c.execute("SELECT date, timestamp, views, likes FROM views WHERE video_id = %s ORDER BY date, timestamp", (video_id,))
        rows = c.fetchall()
        data = []
        for i, row in enumerate(rows):
            date, timestamp, views, likes = row
            view_gain = 0 if i == 0 or rows[i-1][0] != date else views - rows[i-1][2]
            like_gain = 0 if i == 0 or rows[i-1][0] != date else likes - rows[i-1][3]
            view_like_ratio = round(views / likes, 2) if likes > 0 else 0

            view_hourly_gain = 0
            like_hourly_gain = 0
            comp_view_ratio = None
            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

            c.execute("""
                SELECT views FROM views 
                WHERE video_id = %s AND date = %s AND timestamp <= %s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            prev = c.fetchone()
            if prev:
                previous_views = prev[0]
                view_hourly_gain = views - previous_views

            c.execute("""
                SELECT likes FROM views 
                WHERE video_id = %s AND date = %s AND timestamp <= %s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            prev = c.fetchone()
            if prev:
                previous_likes = prev[0]
                like_hourly_gain = likes - previous_likes

            if comparison_video_id:
                c.execute("""
                    SELECT views FROM views 
                    WHERE video_id = %s AND date = %s AND timestamp <= %s
                    ORDER BY timestamp DESC LIMIT 1
                """, (comparison_video_id, date, one_hour_ago))
                comp_prev = c.fetchone()
                if comp_prev:
                    comp_previous_views = comp_prev[0]
                    c.execute("""
                        SELECT views FROM views 
                        WHERE video_id = %s AND date = %s AND timestamp = %s
                    """, (comparison_video_id, date, timestamp))
                    comp_current_result = c.fetchone()
                    if comp_current_result and comp_current_result[0] > 0:
                        comp_view_hourly_gain = comp_current_result[0] - comp_previous_views
                        if comp_view_hourly_gain != 0:
                            comp_view_ratio = round(view_hourly_gain / comp_view_hourly_gain, 2)
                        else:
                            comp_view_ratio = None
                    else:
                        comp_view_ratio = None

            data.append({
                "Date": date,
                "Timestamp": timestamp,
                "Views": views,
                "Likes": likes,
                "View Gain": view_gain,
                "Like Gain": like_gain,
                "View Hourly Gain": view_hourly_gain,
                "Like Hourly Gain": like_hourly_gain,
                "Views/Likes Ratio": view_like_ratio,
                "Comparison View Ratio": comp_view_ratio
            })

        df = pd.DataFrame(data)
        conn.close()

        excel_file = f"youtube_stats_{video_id}.xlsx"
        df.to_excel(excel_file, sheet_name=(name[:31] if name else "sheet1"), index=False, engine="openpyxl")

        return send_file(excel_file, as_attachment=True, download_name=f"{name}_stats.xlsx")
    except Exception as e:
        logger.error(f"Error in export route: {e}", exc_info=True)
        try:
            conn.close()
        except Exception:
            pass
        flash("Error exporting data.", "error")
        return redirect(url_for("index"))


# Initialize database and start background tasks at application startup
init_db()
start_background_tasks()

if __name__ == "__main__":
    # On local run keep debug off for background thread stability; set env var FLASK_DEBUG if needed.
    app.run(host="0.0.0.0", port=5000)
