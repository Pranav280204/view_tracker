# app.py
from flask import Flask, render_template, request, redirect, url_for, flash, send_file
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
import pandas as pd

# === Config ===
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
    "?sslmode=prefer"
)
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

app = Flask(__name__)
app.secret_key = os.urandom(24)
logging.basicConfig(level=logging.INFO)

# === DB ===
@contextmanager
def get_db_cursor():
    conn = psycopg.connect(POSTGRES_URL, row_factory=dict_row)
    try:
        yield conn.cursor()
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

# === YouTube ===
def get_youtube():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY) if YOUTUBE_API_KEY else None

def extract_video_id(link):
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(link)
    if parsed.hostname in ("youtube.com", "www.youtube.com"):
        return parse_qs(parsed.query).get("v", [None])[0]
    if parsed.hostname == "youtu.be":
        return parsed.path[1:]
    return None

def fetch_title(video_id):
    youtube = get_youtube()
    if not youtube: return None
    try:
        resp = youtube.videos().list(part="snippet", id=video_id).execute()
        return resp["items"][0]["snippet"]["title"][:100] if resp["items"] else None
    except: return None

def fetch_views(video_ids):
    youtube = get_youtube()
    if not youtube or not video_ids: return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        return {item["id"]: {
            "views": int(item["statistics"].get("viewCount", 0)),
            "likes": int(item["statistics"].get("likeCount", 0))
        } for item in resp.get("items", [])}
    except: return {}

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
        logging.error(f"Store error: {e}")

# === Data Processing (4 columns only) ===
def process_view_gains(rows):
    if not rows:
        return []
    processed = []
    for i, row in enumerate(rows):
        timestamp, views = row["timestamp"], row["views"]
        view_gain = 0
        view_hourly = 0
        if i > 0 and rows[i-1]["date"] == row["date"]:
            view_gain = views - rows[i-1]["views"]
        # Hourly gain
        ts_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        one_hour_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        for prev in reversed(rows[:i]):
            if prev["date"] != row["date"] or prev["timestamp"] > one_hour_ago:
                continue
            view_hourly = views - prev["views"]
            break
        processed.append((timestamp, views, view_gain, view_hourly))
    return processed

# === Background Polling ===
def start_polling():
    def poll():
        while True:
            ist = pytz.timezone("Asia/Kolkata")
            now = datetime.now(ist)
            wait = (5 - now.minute % 5) * 60 - now.second
            if wait <= 0: wait += 300
            time.sleep(max(wait, 10))
            try:
                with get_db_cursor() as cur:
                    cur.execute("SELECT video_id FROM video_list WHERE is_tracking = 1")
                    video_ids = [r["video_id"] for r in cur.fetchall()]
                if video_ids:
                    stats = fetch_views(video_ids)
                    for vid in video_ids:
                        if vid in stats:
                            store_views(vid, stats[vid])
            except Exception as e:
                logging.error(f"Poll error: {e}")
                time.sleep(60)
    t = threading.Thread(target=poll, daemon=True)
    t.start()

# === Routes ===
@app.route("/")
def index():
    videos = []
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT video_id, name FROM video_list")
            for row in cur.fetchall():
                vid = row["video_id"]
                cur.execute("SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC", (vid,))
                dates = [r["date"] for r in cur.fetchall()]
                daily = {}
                for d in dates:
                    cur.execute("SELECT timestamp, views, date FROM views WHERE video_id=%s AND date=%s ORDER BY timestamp", (vid, d))
                    daily[d] = process_view_gains(cur.fetchall())
                videos.append({"video_id": vid, "name": row["name"], "daily_data": daily})
        return render_template("index.html", videos=videos)
    except Exception as e:
        logging.error(f"Index error: {e}")
        return render_template("index.html", videos=[], error="DB error")

@app.route("/add_video", methods=["POST"])
def add_video():
    link = request.form.get("video_link")
    if not link: 
        flash("Link required", "error")
        return redirect(url_for("index"))
    vid = extract_video_id(link)
    if not vid:
        flash("Invalid link", "error")
        return redirect(url_for("index"))
    title = fetch_title(vid)
    if not title:
        flash("Cannot fetch title", "error")
        return redirect(url_for("index"))
    stats = fetch_views([vid])
    if vid not in stats:
        flash("Cannot fetch stats", "error")
        return redirect(url_for("index"))
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                INSERT INTO video_list (video_id, name, is_tracking)
                VALUES (%s, %s, 1)
                ON CONFLICT (video_id) DO UPDATE SET name=EXCLUDED.name, is_tracking=1
            """, (vid, title))
        store_views(vid, stats[vid])
        flash("Video added", "success")
    except Exception as e:
        flash("DB error", "error")
    return redirect(url_for("index"))

@app.route("/stop/<video_id>")
def stop(video_id):
    with get_db_cursor() as cur:
        cur.execute("UPDATE video_list SET is_tracking=0 WHERE video_id=%s", (video_id,))
    return redirect(url_for("index"))

@app.route("/remove/<video_id>")
def remove(video_id):
    with get_db_cursor() as cur:
        cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
        cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
    return redirect(url_for("index"))

@app.route("/export/<video_id>")
def export(video_id):
    with get_db_cursor() as cur:
        cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
        name = cur.fetchone()["name"]
        cur.execute("SELECT timestamp, views FROM views WHERE video_id=%s ORDER BY timestamp", (video_id,))
        rows = cur.fetchall()
    df = pd.DataFrame(rows)
    df.columns = ["Timestamp", "Views"]
    fname = f"{name}_stats.xlsx"
    df.to_excel(fname, index=False)
    return send_file(fname, as_attachment=True)

# === Init ===
def init_db():
    with get_db_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS video_list (
                video_id TEXT PRIMARY KEY,
                name TEXT,
                is_tracking INTEGER DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS views (
                video_id TEXT,
                date DATE,
                timestamp TIMESTAMP,
                views BIGINT,
                likes BIGINT,
                PRIMARY KEY (video_id, timestamp)
            );
        """)

init_db()
start_polling()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
