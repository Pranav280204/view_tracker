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
import psycopg
from psycopg.rows import dict_row

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(24).hex())

# -------------------------- Logging --------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# -------------------------- YouTube API --------------------------
API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# -------------------------- PostgreSQL --------------------------
POSTGRES_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db",
)

db_conn = None
_background_thread = None
_thread_lock = threading.Lock()


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
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS views (
            video_id TEXT NOT NULL,
            date DATE NOT NULL,
            timestamp TEXT NOT NULL,
            views BIGINT NOT NULL,
            likes BIGINT NOT NULL,
            PRIMARY KEY (video_id, timestamp)
        );
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS video_list (
            video_id TEXT PRIMARY KEY,
            name TEXT,
            is_tracking INTEGER DEFAULT 1
        );
        """
    )
    logger.info("Tables ready")


def extract_video_id(link: str) -> str | None:
    parsed = urlparse(link.strip())
    if parsed.hostname in ("youtube.com", "www.youtube.com"):
        return parse_qs(parsed.query).get("v", [None])[0]
    if parsed.hostname == "youtu.be":
        return parsed.path[1:] if len(parsed.path) > 1 else None
    return None


def fetch_video_title(vid: str) -> str:
    if not youtube:
        return "Unknown Video"
    try:
        resp = youtube.videos().list(part="snippet", id=vid).execute()
        return (
            resp["items"][0]["snippet"]["title"][:100]
            if resp.get("items")
            else "Unknown Video"
        )
    except Exception as e:
        logger.error(f"Title fetch error {vid}: {e}")
        return "Unknown Video"


def fetch_views(ids: list[str]) -> dict:
    if not youtube or not ids:
        return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(ids)).execute()
        return {
            item["id"]: {
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0)),
            }
            for item in resp.get("items", [])
        }
    except Exception as e:
        logger.error(f"YouTube API error: {e}")
        return {}


def safe_store(vid: str, stats: dict):
    cur = get_db().cursor()
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")

    # Remove possible duplicate from same second
    cur.execute("DELETE FROM views WHERE video_id=%s AND timestamp=%s", (vid, ts))
    cur.execute(
        """
        INSERT INTO views (video_id, date, timestamp, views, likes)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (vid, date, ts, stats["views"], stats["likes"]),
    )
    logger.info(f"STORED {vid} → {stats['views']:,} views")


# -------------------------- Background Task --------------------------
def start_background():
    global _background_thread
    with _thread_lock:
        if _background_thread and _background_thread.is_alive():
            return

        def run():
            while True:
                try:
                    ist = pytz.timezone("Asia/Kolkata")
                    now = datetime.now(ist)
                    # Wait until next 5-minute mark
                    seconds_to_next = 300 - (now.minute % 5 * 60 + now.second)
                    if seconds_to_next <= 0:
                        seconds_to_next += 300
                    time.sleep(seconds_to_next)

                    cur = get_db().cursor()
                    cur.execute("SELECT video_id FROM video_list WHERE is_tracking=1")
                    ids = [row["video_id"] for row in cur.fetchall()]

                    if ids:
                        stats = fetch_views(ids)
                        for vid in ids:
                            if vid in stats:
                                safe_store(vid, stats[vid])
                except Exception as e:
                    logger.error(f"Background task error: {e}")
                    time.sleep(60)

        _background_thread = threading.Thread(target=run, daemon=True)
        _background_thread.start()
        logger.info("Background task started")


# -------------------------- Data Processing --------------------------
def process_gains(vid: str, rows: list[dict]) -> list[tuple]:
    if not rows:
        return []

    result = []
    for i, row in enumerate(rows):
        views = row["views"]
        ts = row["timestamp"]
        date = row["date"]

        # 5-min gain
        gain = 0
        if i > 0 and rows[i - 1]["date"] == date:
            gain = views - rows[i - 1]["views"]

        # Hourly gain
        ts_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        one_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        cur = get_db().cursor()
        cur.execute(
            """
            SELECT views FROM views
            WHERE video_id=%s AND date=%s AND timestamp <= %s
            ORDER BY timestamp DESC LIMIT 1
            """,
            (vid, date, one_ago),
        )
        prev = cur.fetchone()
        hourly = views - prev["views"] if prev else 0

        result.append((ts, views, gain, hourly))
    return result


# -------------------------- Routes --------------------------
@app.route("/", methods=["GET"])
def index():
    videos = []
    try:
        cur = get_db().cursor()
        cur.execute("SELECT video_id, name, is_tracking FROM video_list ORDER BY name")
        for row in cur.fetchall():
            vid = row["video_id"]
            cur.execute(
                "SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC",
                (vid,),
            )
            dates = [r["date"] for r in cur.fetchall()]

            daily = {}
            for d in dates:
                cur.execute(
                    """
                    SELECT timestamp, views, date
                    FROM views WHERE video_id=%s AND date=%s
                    ORDER BY timestamp ASC
                    """,
                    (vid, d),
                )
                daily[d] = process_gains(vid, cur.fetchall())

            videos.append(
                {
                    "video_id": vid,
                    "name": row["name"] or "Unknown",
                    "daily_data": daily,
                    "is_tracking": bool(row["is_tracking"]),
                }
            )
        return render_template("index.html", videos=videos)
    except Exception as e:
        logger.error(f"Index error: {e}", exc_info=True)
        flash("Database error – retrying...", "error")
        return render_template("index.html", videos=[])


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
        flash("Could not fetch video stats (private/deleted?)", "error")
        return redirect(url_for("index"))

    cur = get_db().cursor()
    cur.execute(
        """
        INSERT INTO video_list (video_id, name, is_tracking)
        VALUES (%s, %s, 1)
        ON CONFLICT (video_id) DO UPDATE SET name=%s, is_tracking=1
        """,
        (vid, title, title),
    )
    safe_store(vid, stats[vid])
    flash(f"Added: {title[:60]}...", "success")
    return redirect(url_for("index"))


@app.route("/toggle/<video_id>")
def toggle(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT is_tracking FROM video_list WHERE video_id=%s", (video_id,))
    row = cur.fetchone()
    if not row:
        flash("Video not found", "error")
        return redirect(url_for("index"))

    new_state = 0 if row["is_tracking"] else 1
    cur.execute(
        "UPDATE video_list SET is_tracking=%s WHERE video_id=%s",
        (new_state, video_id),
    )
    flash("Paused" if new_state == 0 else "Resumed", "success")
    return redirect(url_for("index"))


@app.route("/remove_video/<video_id>")
def remove(video_id):
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

    name = row["name"] or "export"
    cur.execute(
        "SELECT timestamp, views FROM views WHERE video_id=%s ORDER BY timestamp",
        (video_id,),
    )
    rows = cur.fetchall()
    data = [{"Time": r["timestamp"], "Views": r["views"]} for r in rows]
    df = pd.DataFrame(data)

    fname = f"/tmp/{video_id}_views.xlsx"
    df.to_excel(fname, index=False)
    return send_file(
        fname,
        as_attachment=True,
        download_name=f"{name[:50]}_views.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# -------------------------- Startup --------------------------
# Startup

init_db()

start_background_tasks()

if __name__ == "__main__":

    app.run(debug=True)
