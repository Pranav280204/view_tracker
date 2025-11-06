import os
import threading
import logging
import pytz
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

# NEW: postgres
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- YouTube API setup ---
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY environment variable is not set")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# --- Postgres connection pool ---
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    # Fallback to your provided URL; add sslmode=require for Render
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db?sslmode=require"
)

pg_pool: pool.SimpleConnectionPool | None = None

def init_pool():
    global pg_pool
    if pg_pool is None:
        pg_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL
        )
        logger.info("PostgreSQL connection pool initialized")

@contextmanager
def get_conn():
    """Get a pooled connection and cursor; auto-commit & close safely."""
    conn = None
    try:
        conn = pg_pool.getconn()
        yield conn
    finally:
        if conn:
            pg_pool.putconn(conn)

def run_query(sql, params=None, fetchone=False, fetchall=False):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            if fetchone:
                return cur.fetchone()
            if fetchall:
                return cur.fetchall()
            conn.commit()

# --- DB init (Postgres schemas) ---
def init_db():
    init_pool()
    # create tables
    run_query("""
        CREATE TABLE IF NOT EXISTS views (
            video_id TEXT NOT NULL,
            date      TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            views     BIGINT,
            likes     BIGINT
        );
    """)
    run_query("""
        CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name     TEXT,
            is_tracking INTEGER DEFAULT 1,
            comparison_video_id TEXT
        );
    """)
    logger.info("Successfully initialized PostgreSQL database")

# --- Helpers ---
def extract_video_id(video_link):
    try:
        parsed_url = urlparse(video_link)
        if parsed_url.hostname in ("www.youtube.com", "youtube.com", "youtu.be", "m.youtube.com"):
            if parsed_url.hostname == "youtu.be":
                return parsed_url.path[1:] if len(parsed_url.path) > 1 else None
            query = parse_qs(parsed_url.query)
            return query.get("v", [None])[0]
        return None
    except Exception as e:
        logger.error(f"Error parsing video link: {e}")
        return None

def fetch_video_title(video_id):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return None
    try:
        response = youtube.videos().list(part="snippet", id=video_id).execute()
        for item in response.get("items", []):
            return item["snippet"]["title"][:50]
        return None
    except HttpError as e:
        logger.error(f"Error fetching title for {video_id}: {e}")
        return None

def fetch_views(video_ids):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return {}
    if not video_ids:
        return {}
    try:
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

def store_views(video_id, stats):
    try:
        ist = pytz.timezone("Asia/Kolkata")
        now = datetime.now(ist)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        date = now.strftime("%Y-%m-%d")
        views = stats.get("views", 0)
        likes = stats.get("likes", 0)
        run_query(
            "INSERT INTO views (video_id, date, timestamp, views, likes) VALUES (%s, %s, %s, %s, %s)",
            (video_id, date, timestamp, views, likes)
        )
        logger.debug(f"Stored stats for {video_id}: views={views}, likes={likes} at {timestamp} IST")
    except Exception as e:
        logger.error(f"Error storing stats for {video_id}: {e}")

def fetch_latest_sourav_joshi_video():
    if not youtube:
        logger.error("YouTube API client not initialized")
        return None, None, None
    try:
        response = youtube.search().list(
            part="id,snippet",
            channelId="UCj0Cw6g1v3q4ruQVWdQe2zw",
            maxResults=1,
            order="date",
            type="video"
        ).execute()
        for item in response.get("items", []):
            video_id = item["id"]["videoId"]
            title = item["snippet"]["title"][:50]
            published_at = datetime.strptime(item["snippet"]["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
            published_at = pytz.utc.localize(published_at).astimezone(pytz.timezone("Asia/Kolkata"))
            return video_id, title, published_at
        return None, None, None
    except HttpError as e:
        logger.error(f"Error fetching latest Sourav Joshi video: {e}")
        return None, None, None

def background_tasks():
    last_sourav_check = None
    default_comparison_id = "YxWlaYCA8MU"
    while True:
        try:
            if psutil:
                process = psutil.Process()
                memory_info = process.memory_info()
                logger.debug(f"Memory usage: RSS={memory_info.rss / 1024 / 1024:.2f} MB, VMS={memory_info.vms / 1024 / 1024:.2f} MB")

            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            current_time = now.time()
            current_date = now.date()

            # Check for new Sourav Joshi video at 08:05 IST
            if current_time.hour == 8 and current_time.minute == 5 and (last_sourav_check is None or last_sourav_check.date() != current_date):
                video_id, title, published_at = fetch_latest_sourav_joshi_video()
                if video_id and published_at.date() == current_date:
                    exist = run_query(
                        "SELECT video_id FROM video_list WHERE video_id = %s",
                        (video_id,), fetchone=True
                    )
                    if not exist:
                        # Stop tracking previous Sourav Joshi videos that match name pattern
                        run_query("""
                            UPDATE video_list 
                            SET is_tracking = 0 
                            WHERE video_id != %s AND name LIKE %s
                        """, (video_id, '%Sourav Joshi%'))

                        # Upsert new video with default comparison
                        run_query("""
                            INSERT INTO video_list (video_id, name, is_tracking, comparison_video_id)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (video_id) DO UPDATE
                            SET name = EXCLUDED.name,
                                is_tracking = EXCLUDED.is_tracking,
                                comparison_video_id = EXCLUDED.comparison_video_id
                        """, (video_id, title, 1, default_comparison_id))

                        logger.info(f"Added Sourav Joshi video: {video_id} - {title} with default comparison {default_comparison_id}, stopped tracking previous videos")

                        stats = fetch_views([video_id, default_comparison_id])
                        if stats.get(video_id):
                            store_views(video_id, stats[video_id])
                        if stats.get(default_comparison_id):
                            store_views(default_comparison_id, stats[default_comparison_id])
                last_sourav_check = now

            # sleep till next 5-minute mark
            current_seconds = now.minute * 60 + now.second + (now.microsecond / 1_000_000)
            seconds_to_wait = (300 - (current_seconds % 300)) or 300
            logger.debug(f"Current time: {now}, sleeping {seconds_to_wait:.2f}s to next 5-min mark")
            time.sleep(seconds_to_wait)

            # Fetch stats for all actively tracked videos and their comparisons
            video_pairs = run_query(
                "SELECT video_id, comparison_video_id FROM video_list WHERE is_tracking = 1",
                fetchall=True
            ) or []
            video_ids = [v[0] for v in video_pairs]
            comparison_ids = [v[1] for v in video_pairs if v[1]]
            all_ids = list(set(video_ids + comparison_ids))
            if not all_ids:
                logger.debug("No videos to fetch stats for")
                continue

            stats_dict = fetch_views(all_ids)
            for vid in video_ids:
                if stats_dict.get(vid):
                    store_views(vid, stats_dict[vid])
            for comp in comparison_ids:
                if stats_dict.get(comp):
                    store_views(comp, stats_dict[comp])
        except Exception as e:
            logger.error(f"Background task error: {e}")

def start_background_tasks():
    thread = threading.Thread(target=background_tasks, daemon=True)
    thread.start()

def process_view_gains(video_id, data, comparison_video_id=None):
    processed_data = []
    for i, (date, timestamp, views, likes) in enumerate(data):
        view_gain = 0 if i == 0 or data[i-1][0] != date else views - data[i-1][2]
        like_gain = 0 if i == 0 or data[i-1][0] != date else likes - data[i-1][3]
        view_like_ratio = round(views / likes, 2) if likes and likes > 0 else 0

        view_hourly_gain = 0
        like_hourly_gain = 0
        comp_view_ratio = None

        timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

        # previous views for hourly gain
        prev_v = run_query("""
            SELECT views FROM views 
            WHERE video_id = %s AND date = %s AND timestamp <= %s 
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, date, one_hour_ago), fetchone=True)
        if prev_v:
            view_hourly_gain = views - prev_v[0]

        prev_l = run_query("""
            SELECT likes FROM views 
            WHERE video_id = %s AND date = %s AND timestamp <= %s 
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, date, one_hour_ago), fetchone=True)
        if prev_l:
            like_hourly_gain = likes - prev_l[0]

        # comparison ratio
        if comparison_video_id:
            comp_prev_v = run_query("""
                SELECT views FROM views 
                WHERE video_id = %s AND date = %s AND timestamp <= %s 
                ORDER BY timestamp DESC LIMIT 1
            """, (comparison_video_id, date, one_hour_ago), fetchone=True)
            if comp_prev_v:
                comp_current_v = run_query("""
                    SELECT views FROM views
                    WHERE video_id = %s AND date = %s AND timestamp = %s
                """, (comparison_video_id, date, timestamp), fetchone=True)
                if comp_current_v and comp_current_v[0] > 0:
                    comp_view_hourly_gain = comp_current_v[0] - comp_prev_v[0]
                    if comp_view_hourly_gain != 0:
                        comp_view_ratio = round(view_hourly_gain / comp_view_hourly_gain, 2)

        processed_data.append((
            timestamp, views, likes, view_gain, like_gain, view_hourly_gain,
            view_like_ratio, like_hourly_gain, comp_view_ratio
        ))
    return processed_data

# --- Routes ---
@app.route("/", methods=["GET"])
def index():
    error_message = None
    videos = []
    try:
        video_list = run_query("SELECT video_id, name, is_tracking, comparison_video_id FROM video_list", fetchall=True) or []

        for video_id, name, is_tracking, comparison_video_id in video_list:
            dates_rows = run_query("SELECT DISTINCT date FROM views WHERE video_id = %s ORDER BY date DESC", (video_id,), fetchall=True) or []
            dates = [r[0] for r in dates_rows]
            daily_data = {}
            for d in dates:
                rows = run_query("""
                    SELECT date, timestamp, views, likes
                    FROM views
                    WHERE video_id = %s AND date = %s
                    ORDER BY timestamp ASC
                """, (video_id, d), fetchall=True) or []
                daily_data[d] = process_view_gains(video_id, rows, comparison_video_id)

            videos.append({
                "video_id": video_id,
                "name": name,
                "daily_data": daily_data,
                "is_tracking": bool(is_tracking),
                "comparison_video_id": comparison_video_id
            })

        return render_template("index.html", videos=videos, error_message=error_message)

    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        return render_template("index.html", videos=[], error_message=str(e))

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

        # Upsert into video_list
        run_query("""
            INSERT INTO video_list (video_id, name, is_tracking, comparison_video_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE
            SET name = EXCLUDED.name,
                is_tracking = EXCLUDED.is_tracking,
                comparison_video_id = EXCLUDED.comparison_video_id
        """, (video_id, title, 1, comparison_video_id))

        # Store current snapshot(s)
        store_views(video_id, stats[video_id])
        if comparison_video_id:
            store_views(comparison_video_id, stats[comparison_video_id])

        flash("Video added successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error adding video: {e}")
        flash(str(e), "error")
        return redirect(url_for("index"))

@app.route("/stop_tracking/<video_id>")
def stop_tracking(video_id):
    try:
        run_query("UPDATE video_list SET is_tracking = 0 WHERE video_id = %s", (video_id,))
        flash("Stopped tracking video successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error stopping tracking for video {video_id}: {e}")
        flash(str(e), "error")
        return redirect(url_for("index"))

@app.route("/remove_video/<video_id>")
def remove_video(video_id):
    try:
        result = run_query("SELECT comparison_video_id FROM video_list WHERE video_id = %s", (video_id,), fetchone=True)
        comparison_video_id = result[0] if result else None

        run_query("DELETE FROM video_list WHERE video_id = %s", (video_id,))
        run_query("DELETE FROM views WHERE video_id = %s", (video_id,))
        if comparison_video_id:
            run_query("DELETE FROM views WHERE video_id = %s", (comparison_video_id,))

        flash("Video removed successfully.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Error removing video: {e}")
        flash(str(e), "error")
        return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export(video_id):
    try:
        res = run_query("SELECT name, comparison_video_id FROM video_list WHERE video_id = %s", (video_id,), fetchone=True)
        if not res:
            flash("Video not found.", "error")
            return redirect(url_for("index"))
        name, comparison_video_id = res

        rows = run_query("""
            SELECT date, timestamp, views, likes
            FROM views
            WHERE video_id = %s
            ORDER BY date, timestamp
        """, (video_id,), fetchall=True) or []

        data = []
        for i, row in enumerate(rows):
            date, timestamp, views, likes = row
            prev_row = rows[i-1] if i > 0 and rows[i-1][0] == date else None
            view_gain = (views - prev_row[2]) if prev_row else 0
            like_gain = (likes - prev_row[3]) if prev_row else 0
            view_like_ratio = round(views / likes, 2) if likes and likes > 0 else 0

            timestamp_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            one_hour_ago = (timestamp_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

            prev_v = run_query("""
                SELECT views FROM views 
                WHERE video_id = %s AND date = %s AND timestamp <= %s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago), fetchone=True)
            view_hourly_gain = views - prev_v[0] if prev_v else 0

            prev_l = run_query("""
                SELECT likes FROM views 
                WHERE video_id = %s AND date = %s AND timestamp <= %s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago), fetchone=True)
            like_hourly_gain = likes - prev_l[0] if prev_l else 0

            comp_view_ratio = None
            if comparison_video_id:
                comp_prev_v = run_query("""
                    SELECT views FROM views 
                    WHERE video_id = %s AND date = %s AND timestamp <= %s 
                    ORDER BY timestamp DESC LIMIT 1
                """, (comparison_video_id, date, one_hour_ago), fetchone=True)
                if comp_prev_v:
                    comp_current_v = run_query("""
                        SELECT views FROM views
                        WHERE video_id = %s AND date = %s AND timestamp = %s
                    """, (comparison_video_id, date, timestamp), fetchone=True)
                    if comp_current_v and comp_current_v[0] > 0:
                        comp_view_hourly_gain = comp_current_v[0] - comp_prev_v[0]
                        if comp_view_hourly_gain != 0:
                            comp_view_ratio = round(view_hourly_gain / comp_view_hourly_gain, 2)

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
        excel_file = f"youtube_stats_{video_id}.xlsx"
        df.to_excel(excel_file, sheet_name=name[:31], index=False, engine="openpyxl")

        return send_file(excel_file, as_attachment=True, download_name=f"{name}_stats.xlsx")
    except Exception as e:
        logger.error(f"Error in export route: {e}", exc_info=True)
        flash("Error exporting data.", "error")
        return redirect(url_for("index"))

# ---- App bootstrap ----
init_db()
start_background_tasks()

if __name__ == "__main__":
    app.run(debug=True)
