# app_viewer.py
from flask import Flask, render_template
from contextlib import contextmanager
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import threading
import time
import pytz
import logging

# === Configuration ===
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
    "?sslmode=prefer"
)

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
if not YOUTUBE_API_KEY:
    logging.warning("YOUTUBE_API_KEY not set – stats will not update")

# === Flask App ===
app = Flask(__name__)

# === Database Context Manager ===
@contextmanager
def get_db_cursor():
    conn = psycopg.connect(POSTGRES_URL, row_factory=dict_row)
    try:
        yield conn.cursor()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# === YouTube API Helper ===
def get_youtube_client():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY) if YOUTUBE_API_KEY else None

def fetch_views(video_ids):
    if not video_ids or not YOUTUBE_API_KEY:
        return {}
    youtube = get_youtube_client()
    if not youtube:
        return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        stats = {}
        for item in resp.get("items", []):
            vid = item["id"]
            stats[vid] = {
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0))
            }
        return stats
    except HttpError as e:
        logging.error(f"YouTube API error: {e}")
        return {}

def store_views(video_id, stats):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO views (video_id, date, timestamp, views, likes)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (video_id, timestamp) DO NOTHING
            """, (video_id, date, timestamp, stats.get("views", 0), stats.get("likes", 0)))
    except Exception as e:
        logging.error(f"Failed to store views for {video_id}: {e}")

# === Data Processing ===
def process_view_gains(video_id, rows, comparison_video_id=None):
    if not rows:
        return []
    processed = []
    with get_db_cursor() as cur:
        for i, row in enumerate(rows):
            date, timestamp, views, likes = row["date"], row["timestamp"], row["views"], row["likes"]
            # Gains since previous entry on same day
            view_gain = 0
            like_gain = 0
            if i > 0 and rows[i-1]["date"] == date:
                view_gain = views - rows[i-1]["views"]
                like_gain = likes - rows[i-1]["likes"]
            view_like_ratio = round(views / likes, 2) if likes else 0

            # Hourly gains
            ts_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            one_hour_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            view_hourly = like_hourly = 0

            cur.execute("""
                SELECT views FROM views
                WHERE video_id=%s AND date=%s AND timestamp<=%s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            r = cur.fetchone()
            if r:
                view_hourly = views - r["views"]

            cur.execute("""
                SELECT likes FROM views
                WHERE video_id=%s AND date=%s AND timestamp<=%s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            r = cur.fetchone()
            if r:
                like_hourly = likes - r["likes"]

            # Comparison ratio
            comp_ratio = None
            if comparison_video_id:
                cur.execute("""
                    SELECT views FROM views
                    WHERE video_id=%s AND date=%s AND timestamp<=%s
                    ORDER BY timestamp DESC LIMIT 1
                """, (comparison_video_id, date, one_hour_ago))
                prev = cur.fetchone()
                cur.execute("""
                    SELECT views FROM views
                    WHERE video_id=%s AND date=%s AND timestamp=%s
                """, (comparison_video_id, date, timestamp))
                curr = cur.fetchone()
                if prev and curr and (curr["views"] - prev["views"]) != 0:
                    comp_ratio = round(view_hourly / (curr["views"] - prev["views"]), 2)

            processed.append((
                timestamp, views, likes, view_gain, like_gain,
                view_hourly, view_like_ratio, like_hourly, comp_ratio
            ))
    return processed

# === Background Polling Task ===
def start_background_polling():
    def poll():
        while True:
            try:
                # Sleep until next 5-minute mark
                ist = pytz.timezone("Asia/Kolkata")
                now = datetime.now(ist)
                minutes = now.minute % 5
                seconds = now.second + now.microsecond / 1_000_000
                wait = (5 - minutes) * 60 - seconds
                if wait <= 0:
                    wait += 300
                time.sleep(max(wait, 10))  # Minimum 10s

                # Fetch tracked videos
                with get_db_cursor() as cur:
                    cur.execute("""
                        SELECT video_id, comparison_video_id
                        FROM video_list
                        WHERE is_tracking = 1
                    """)
                    rows = cur.fetchall()

                video_ids = [r["video_id"] for r in rows]
                comp_ids = [r["comparison_video_id"] for r in rows if r["comparison_video_id"]]
                all_ids = list(set(video_ids + comp_ids))

                if all_ids:
                    stats = fetch_views(all_ids)
                    for vid in video_ids:
                        if vid in stats:
                            store_views(vid, stats[vid])
                    for cid in comp_ids:
                        if cid in stats:
                            store_views(cid, stats[cid])

            except Exception as e:
                logging.error(f"Background polling error: {e}")
                time.sleep(60)

    t = threading.Thread(target=poll, daemon=True)
    t.start()
    logging.info("Background polling started")

# === Route ===
@app.route("/")
def viewer():
    videos = []
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT video_id, name, comparison_video_id
                FROM video_list
                WHERE is_tracking = 1
                ORDER BY name
            """)
            for row in cur.fetchall():
                vid = row["video_id"]
                cur.execute("""
                    SELECT DISTINCT date FROM views
                    WHERE video_id=%s
                    ORDER BY date DESC
                """, (vid,))
                dates = [r["date"] for r in cur.fetchall()]

                daily_data = {}
                for d in dates:
                    cur.execute("""
                        SELECT date, timestamp, views, likes
                        FROM views
                        WHERE video_id=%s AND date=%s
                        ORDER BY timestamp ASC
                    """, (vid, d))
                    rows = cur.fetchall()
                    daily_data[d] = process_view_gains(vid, rows, row["comparison_video_id"])

                videos.append({
                    "video_id": vid,
                    "name": row["name"],
                    "daily_data": daily_data
                })

        return render_template("viewer.html", videos=videos)

    except Exception as e:
        logging.error(f"Viewer route error: {e}")
        return render_template("viewer.html", videos=[], error_message="Service temporarily unavailable.")

# === Startup ===
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    start_background_polling()
    app.run(host="0.0.0.0", port=5000)
else:
    # When run via gunicorn
    start_background_polling()
