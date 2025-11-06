# app.py - FINAL FIXED VERSION (NO DUPLICATES + VIEWS-ONLY TRACKING)

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
    logging.warning("psutil not found – memory monitoring disabled")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", os.urandom(24).hex())

# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# YouTube API
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY not set")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# PostgreSQL
POSTGRES_URL = os.getenv("DATABASE_URI",
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
)

db_conn = None
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
            timestamp TIMESTAMP NOT NULL,
            views BIGINT NOT NULL,
            PRIMARY KEY (video_id, timestamp)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_tracking INTEGER DEFAULT 1,
            comparison_video_id TEXT
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_views_video_date ON views(video_id, date);")
    conn.commit()
    logger.info("Database ready")

# Helpers
def extract_video_id(link):
    parsed = urlparse(link)
    if parsed.hostname in ("www.youtube.com", "youtube.com"):
        return parse_qs(parsed.query).get("v", [None])[0]
    if parsed.hostname == "youtu.be":
        return parsed.path[1:] if len(parsed.path) > 1 else None
    return None

def fetch_video_title(vid):
    if not youtube: return "Unknown"
    try:
        resp = youtube.videos().list(part="snippet", id=vid).execute()
        return resp["items"][0]["snippet"]["title"][:50] if resp.get("items") else "Unknown"
    except Exception as e:
        logger.error(f"Title error {vid}: {e}")
        return "Unknown"

def fetch_views(video_ids):
    if not youtube or not video_ids: return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        stats = {}
        for item in resp.get("items", []):
            vid = item["id"]
            stats[vid] = {"views": int(item["statistics"].get("viewCount", 0))}
        return stats
    except Exception as e:
        logger.error(f"Stats fetch error: {e}")
        return {}

def store_views(video_id, views_count):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    date = now.strftime("%Y-%m-%d")
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    cur = get_db().cursor()
    cur.execute("""
        INSERT INTO views (video_id, date, timestamp, views)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (video_id, timestamp) DO NOTHING
    """, (video_id, date, timestamp, views_count))

# Background task - 5-minute sync + NO DUPLICATES
def background_tasks():
    while True:
        try:
            now = datetime.now(pytz.timezone("Asia/Kolkata"))
            wait = (5 - (now.minute % 5)) * 60 - now.second - now.microsecond/1e6
            if wait <= 0: wait += 300
            time.sleep(max(1, wait))

            cur = get_db().cursor()
            cur.execute("SELECT video_id, comparison_video_id FROM video_list WHERE is_tracking = 1")
            rows = cur.fetchall()
            main_ids = [r["video_id"] for r in rows]
            comp_ids = [r["comparison_video_id"] for r in rows if r["comparison_video_id"]]
            all_ids = list(set(main_ids + comp_ids))

            if all_ids:
                stats = fetch_views(all_ids)
                stored = set()
                for vid in all_ids:
                    if vid and vid in stats and vid not in stored:
                        store_views(vid, stats[vid]["views"])
                        stored.add(vid)

            if psutil:
                logger.debug(f"Memory: {psutil.Process().memory_info().rss/1024/1024:.1f} MB")
        except Exception as e:
            logger.error(f"Background error: {e}")

def start_background_tasks():
    t = threading.Thread(target=background_tasks, daemon=True)
    t.start()

# Process data (views only)
def process_video_data(video_id):
    cur = get_db().cursor()
    cur.execute("""
        SELECT date, timestamp, views
        FROM views WHERE video_id=%s
        ORDER BY timestamp
    """, (video_id,))
    rows = cur.fetchall()
    result = []
    prev_daily = None
    for row in rows:
        views = row["views"]
        timestamp = row["timestamp"]
        date = row["date"]

        # 5-min gain
        gain_5min = 0
        if prev_daily and prev_daily["date"] == date:
            gain_5min = views - prev_daily["views"]
        prev_daily = row

        # Hourly gain
        ts_dt = datetime.strptime(str(timestamp), "%Y-%m-%d %H:%M:%S")
        one_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        cur.execute("""
            SELECT views FROM views
            WHERE video_id=%s AND timestamp<=%s AND date=%s
            ORDER BY timestamp DESC LIMIT 1
        """, (video_id, one_ago, date))
        prev = cur.fetchone()
        hourly_gain = views - prev["views"] if prev else 0

        result.append((timestamp, views, gain_5min, hourly_gain))
    return result

# Routes
@app.route("/")
def index():
    videos = []
    cur = get_db().cursor()
    cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
    for row in cur.fetchall():
        data = process_video_data(row["video_id"])
        daily = {}
        for ts, views, g5, gh in data:
            date = str(ts).split()[0]
            daily.setdefault(date, []).append((ts, views, g5, gh))
        videos.append({
            "video_id": row["video_id"],
            "name": row["name"],
            "is_tracking": bool(row["is_tracking"]),
            "daily_data": daily
        })
    return render_template("index.html", videos=videos)

@app.route("/add_video", methods=["POST"])
def add_video():
    vlink = request.form.get("video_link")
    clink = request.form.get("comparison_link", "").strip()
    if not vlink:
        flash("Video link required", "error")
        return redirect(url_for("index"))

    vid = extract_video_id(vlink)
    if not vid:
        flash("Invalid YouTube link", "error")
        return redirect(url_for("index"))

    title = fetch_video_title(vid)
    comp_id = extract_video_id(clink) if clink else None

    stats = fetch_views([vid] + ([comp_id] if comp_id else []))
    if vid not in stats:
        flash("Could not fetch video stats", "error")
        return redirect(url_for("index"))

    cur = get_db().cursor()
    cur.execute("""
        INSERT INTO video_list (video_id, name, is_tracking, comparison_video_id)
        VALUES (%s, %s, 1, %s)
        ON CONFLICT (video_id) DO UPDATE SET name=EXCLUDED.name, is_tracking=1
    """, (vid, title, comp_id))

    store_views(vid, stats[vid]["views"])
    if comp_id and comp_id in stats:
        store_views(comp_id, stats[comp_id]["views"])

    flash("Video added successfully!", "success")
    return redirect(url_for("index"))

@app.route("/stop_tracking/<video_id>")
def stop_tracking(video_id):
    get_db().cursor().execute("UPDATE video_list SET is_tracking=0 WHERE video_id=%s", (video_id,))
    flash("Tracking stopped", "success")
    return redirect(url_for("index"))

@app.route("/remove_video/<video_id>")
def remove_video(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT comparison_video_id FROM video_list WHERE video_id=%s", (video_id,))
    comp = cur.fetchone()
    cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
    if comp and comp["comparison_video_id"]:
        cur.execute("DELETE FROM views WHERE video_id=%s", (comp["comparison_video_id"],))
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

    data = process_video_data(video_id)
    rows = []
    for ts, views, g5, gh in data:
        rows.append({
            "Timestamp (IST)": ts,
            "Views": views,
            "View Gain (5-min)": g5,
            "Hourly View Gain": gh
        })
    df = pd.DataFrame(rows)
    fname = f"{video_id}_stats.xlsx"
    df.to_excel(fname, index=False, engine="openpyxl")
    return send_file(fname, as_attachment=True, download_name=f"{name}_views.xlsx")

# Start
init_db()
start_background_tasks()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=False)
