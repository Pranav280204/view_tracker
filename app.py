# app.py
import os
import threading
import logging
import pytz
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
from flask import Flask, render_template, request, redirect, url_for, flash
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row

# Optional: memory monitoring
try:
    import psutil
except ImportError:
    psutil = None
    logging.warning("psutil not found – memory monitoring disabled")

# Flask
app = Flask(__name__)
app.secret_key = os.urandom(24)

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# YouTube API
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.warning("YOUTUBE_API_KEY not set – YouTube calls disabled")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# PostgreSQL (Render)
POSTGRES_URL = (
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
    "?sslmode=prefer"
)

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

# DB Init
def init_db():
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS video_list (
                    video_id TEXT PRIMARY KEY,
                    name TEXT,
                    is_tracking INTEGER DEFAULT 1
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS views (
                    video_id TEXT,
                    date DATE,
                    timestamp TIMESTAMP,
                    views BIGINT,
                    PRIMARY KEY (video_id, timestamp)
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_views_date ON views(date);")
        logger.info("DB tables initialized")
    except Exception as e:
        logger.warning(f"DB init skipped: {e}")

# Helpers
def extract_video_id(link):
    try:
        parsed = urlparse(link)
        if parsed.hostname in ("youtube.com", "www.youtube.com"):
            return parse_qs(parsed.query).get("v", [None])[0]
        if parsed.hostname == "youtu.be":
            return parsed.path[1:] if len(parsed.path) > 1 else None
        return None
    except:
        return None

def fetch_title(vid):
    if not youtube: return None
    try:
        resp = youtube.videos().list(part="snippet", id=vid).execute()
        return resp["items"][0]["snippet"]["title"][:100] if resp["items"] else None
    except HttpError as e:
        logger.error(f"Title fetch failed: {e}")
        return None

def fetch_views(vids):
    if not youtube or not vids: return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(vids)).execute()
        return {
            item["id"]: {"views": int(item["statistics"].get("viewCount", 0))}
            for item in resp.get("items", [])
        }
    except HttpError as e:
        logger.error(f"Stats fetch failed: {e}")
        return {}

def store_views(vid, views_count):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO views (video_id, date, timestamp, views)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (video_id, timestamp) DO NOTHING
            """, (vid, date, ts, views_count))
    except Exception as e:
        logger.error(f"Store failed: {e}")

# In-Memory Gain Calculation (4 columns)
def calc_gains(rows):
    if not rows: return []
    out = []
    for i, r in enumerate(rows):
        ts, views, date = r["timestamp"], r["views"], r["date"]
        gain = 0
        hourly = 0
        if i > 0 and rows[i-1]["date"] == date:
            gain = views - rows[i-1]["views"]
        # Hourly gain
        try:
            now_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            hour_ago = (now_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            for prev in reversed(rows[:i]):
                if prev["date"] != date: break
                if prev["timestamp"] <= hour_ago:
                    hourly = views - prev["views"]
                    break
        except: hourly = 0
        out.append((ts, views, gain, hourly))
    return out

# Background Polling (every 5 mins)
def start_polling():
    def poll():
        while True:
            try:
                now = datetime.now(pytz.timezone("Asia/Kolkata"))
                wait = (5 - now.minute % 5) * 60 - now.second
                if wait <= 0: wait += 300
                time.sleep(max(wait, 10))

                with get_db_cursor() as cur:
                    cur.execute("SELECT video_id FROM video_list WHERE is_tracking = 1")
                    vids = [r["video_id"] for r in cur.fetchall()]
                if vids:
                    stats = fetch_views(vids)
                    for vid in vids:
                        if vid in stats:
                            store_views(vid, stats[vid]["views"])
            except Exception as e:
                logger.error(f"Polling error: {e}")
                time.sleep(60)
    t = threading.Thread(target=poll, daemon=True)
    t.start()

# Routes
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        link = request.form.get("video_link")
        if not link:
            flash("Enter a YouTube link.", "error")
            return redirect(url_for("index"))
        vid = extract_video_id(link)
        if not vid:
            flash("Invalid YouTube link.", "error")
            return redirect(url_for("index"))
        title = fetch_title(vid)
        if not title:
            flash("Could not fetch video title.", "error")
            return redirect(url_for("index"))
        stats = fetch_views([vid])
        if vid not in stats:
            flash("Could not fetch views.", "error")
            return redirect(url_for("index"))

        try:
            with get_db_cursor() as cur:
                cur.execute("""
                    INSERT INTO video_list (video_id, name, is_tracking)
                    VALUES (%s, %s, 1)
                    ON CONFLICT (video_id) DO UPDATE SET name=EXCLUDED.name, is_tracking=1
                """, (vid, title))
            store_views(vid, stats[vid]["views"])
            flash("Video added!", "success")
        except Exception as e:
            logger.error(f"DB error: {e}")
            flash("Failed to add video.", "error")
        return redirect(url_for("index"))

    # GET: Display videos
    videos = []
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
            for row in cur.fetchall():
                vid = row["video_id"]
                cur.execute("SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC", (vid,))
                dates = [r["date"] for r in cur.fetchall()]
                daily = {}
                for d in dates:
                    cur.execute("""
                        SELECT date, timestamp, views
                        FROM views WHERE video_id=%s AND date=%s
                        ORDER BY timestamp
                    """, (vid, d))
                    daily[d] = calc_gains(cur.fetchall())
                videos.append({
                    "video_id": vid,
                    "name": row["name"],
                    "daily_data": daily,
                    "is_tracking": bool(row["is_tracking"])
                })
    except Exception as e:
        logger.error(f"Index error: {e}")
    return render_template("index.html", videos=videos)

@app.route("/stop/<video_id>")
def stop(video_id):
    with get_db_cursor() as cur:
        cur.execute("UPDATE video_list SET is_tracking=0 WHERE video_id=%s", (video_id,))
    flash("Tracking stopped.", "success")
    return redirect(url_for("index"))

@app.route("/remove/<video_id>")
def remove(video_id):
    with get_db_cursor() as cur:
        cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
        cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
    flash("Video removed.", "success")
    return redirect(url_for("index"))

# Startup
init_db()
start_polling()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
