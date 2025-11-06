# app.py
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
import psycopg
from psycopg.rows import dict_row

try:
    import psutil
except ImportError:
    psutil = None

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# YouTube API
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY not set")
    youtube = None
else:
    youtube = build("youtube", "v3", developerKey=API_KEY)

# PostgreSQL
POSTGRES_URL = os.getenv("DATABASE_URL", 
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db")

db_conn = None
_background_thread = None  # Singleton thread

def get_db():
    global db_conn
    if db_conn is None or db_conn.closed:
        db_conn = psycopg.connect(
            POSTGRES_URL,
            row_factory=dict_row,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        db_conn.autocommit = True
    return db_conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS views (
            video_id TEXT NOT NULL,
            date DATE NOT NULL,
            timestamp TEXT NOT NULL,
            views BIGINT NOT NULL,
            likes BIGINT NOT NULL,
            PRIMARY KEY (video_id, timestamp)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_tracking INTEGER DEFAULT 1
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_views_date ON views(date);")
    logger.info("PostgreSQL tables ready")

def extract_video_id(link):
    try:
        parsed = urlparse(link)
        if parsed.hostname in ("youtube.com", "www.youtube.com", "youtu.be"):
            if "v=" in parsed.query:
                return parse_qs(parsed.query)["v"][0]
            if parsed.hostname == "youtu.be":
                return parsed.path[1:]
        return None
    except:
        return None

def fetch_video_title(vid):
    if not youtube: return "Unknown"
    try:
        resp = youtube.videos().list(part="snippet", id=vid).execute()
        return resp["items"][0]["snippet"]["title"][:50] if resp["items"] else "Unknown"
    except:
        return "Unknown"

def fetch_views(video_ids):
    if not youtube or not video_ids: return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        return {
            item["id"]: {
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0))
            }
            for item in resp.get("items", [])
        }
    except Exception as e:
        logger.error(f"API Error: {e}")
        return {}

# DUPLICATE-PROOF + SAFE
def safe_store(video_id, stats):
    conn = get_db()
    cur = conn.cursor()
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")

    # Remove any old entry for this exact second
    cur.execute("DELETE FROM views WHERE video_id=%s AND timestamp=%s", (video_id, timestamp))
    
    # Insert fresh
    cur.execute("""
        INSERT INTO views (video_id, date, timestamp, views, likes)
        VALUES (%s, %s, %s, %s, %s)
    """, (video_id, date, timestamp, stats["views"], stats["likes"]))
    
    logger.info(f"Stored → {video_id} | {stats['views']:,} views")

# SINGLETON BACKGROUND TASK
def start_background_task():
    global _background_thread
    if _background_thread is not None:
        logger.info("Background task already running")
        return

    def task():
        while True:
            try:
                now = datetime.now(pytz.timezone("Asia/Kolkata"))
                wait = 300 - (now.minute % 5 * 60 + now.second)
                if wait <= 0:
                    wait += 300
                logger.debug(f"Sleeping {wait}s...")
                time.sleep(wait)

                cur = get_db().cursor()
                cur.execute("SELECT video_id FROM video_list WHERE is_tracking = 1")
                video_ids = [r["video_id"] for r in cur.fetchall()]
                
                if video_ids:
                    stats = fetch_views(video_ids)
                    for vid in video_ids:
                        if vid in stats:
                            safe_store(vid, stats[vid])
            except Exception as e:
                logger.error(f"Background crash: {e}")
                time.sleep(60)

    _background_thread = threading.Thread(target=task, daemon=True)
    _background_thread.start()
    logger.info("Background task STARTED (only once)")

# Process gains
def process_gains(video_id, rows):
    if not rows:
        return []
    processed = []
    for i, row in enumerate(rows):
        views = row["views"]
        timestamp = row["timestamp"]
        date = row["date"]

        # Gain since last poll
        view_gain = 0
        if i > 0 and rows[i-1]["date"] == date:
            view_gain = views - rows[i-1]["views"]

        # Hourly gain
        ts_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        one_hour_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        cur = get_db().cursor()
        cur.execute("""
            SELECT views FROM views
            WHERE video_id=%s AND date=%s AND timestamp <= %s
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, date, one_hour_ago))
        prev = cur.fetchone()
        hourly_gain = views - prev["views"] if prev else 0

        processed.append((timestamp, views, view_gain, hourly_gain))
    return processed

# Routes
@app.route("/", methods=["GET"])
def index():
    videos = []
    try:
        cur = get_db().cursor()
        cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
        for row in cur.fetchall():
            vid = row["video_id"]
            cur.execute("SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC", (vid,))
            dates = [r["date"] for r in cur.fetchall()]
            daily = {}
            for d in dates:
                cur.execute("""
                    SELECT timestamp, views, date
                    FROM views WHERE video_id=%s AND date=%s
                    ORDER BY timestamp ASC
                """, (vid, d))
                daily[d] = process_gains(vid, cur.fetchall())
            videos.append({
                "video_id": vid,
                "name": row["name"],
                "daily_data": daily,
                "is_tracking": bool(row["is_tracking"])
            })
        return render_template("index.html", videos=videos)
    except Exception as e:
        logger.error(f"Index error: {e}")
        return render_template("index.html", videos=[], error_message="DB Error")

@app.route("/add_video", methods=["POST"])
def add_video():
    link = request.form.get("video_link", "").strip()
    if not link:
        flash("Enter a YouTube link", "error")
        return redirect(url_for("index"))

    vid = extract_video_id(link)
    if not vid:
        flash("Invalid YouTube link", "error")
        return redirect(url_for("index"))

    title = fetch_video_title(vid)
    stats = fetch_views([vid])
    if vid not in stats:
        flash("Cannot fetch video stats", "error")
        return redirect(url_for("index"))

    cur = get_db().cursor()
    cur.execute("""
        INSERT INTO video_list (video_id, name, is_tracking)
        VALUES (%s, %s, 1)
        ON CONFLICT (video_id) DO UPDATE SET name=%s, is_tracking=1
    """, (vid, title, title))
    
    safe_store(vid, stats[vid])
    flash(f"Added: {title}", "success")
    return redirect(url_for("index"))

@app.route("/stop_tracking/<video_id>")
def toggle_tracking(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT is_tracking FROM video_list WHERE video_id=%s", (video_id,))
    current = cur.fetchone()
    new_state = 0 if current and current["is_tracking"] else 1
    cur.execute("UPDATE video_list SET is_tracking=%s WHERE video_id=%s", (new_state, video_id))
    flash("Paused" if new_state == 0 else "Resumed", "success")
    return redirect(url_for("index"))

@app.route("/remove_video/<video_id>")
def remove_video(video_id):
    cur = get_db().cursor()
    cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
    cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
    flash("Video removed", "success")
    return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
    row = cur.fetchone()
    if not row:
        flash("Video not found", "error")
        return redirect(url_for("index"))
    
    name = row["name"]
    cur.execute("SELECT date, timestamp, views FROM views WHERE video_id=%s ORDER BY timestamp", (video_id,))
    rows = cur.fetchall()
    
    data = []
    for i, r in enumerate(rows):
        gain = views = r["views"]
        if i > 0 and rows[i-1]["date"] == r["date"]:
            gain = views - rows[i-1]["views"]
        data.append({"Date": r["date"], "Time": r["timestamp"], "Views": views, "Gain": gain})
    
    df = pd.DataFrame(data)
    fname = f"{video_id}.xlsx"
    df.to_excel(fname, index=False, engine="openpyxl")
    return send_file(fname, as_attachment=True, download_name=f"{name}_stats.xlsx")

# Startup
init_db()
start_background_task()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
