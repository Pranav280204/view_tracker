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

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# YouTube API
API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# PostgreSQL URL
POSTGRES_URL = os.getenv("DATABASE_URL",
    "postgresql://youtube_oylg_user:hpBIs9shV2QtNUtvpMSa1opUa8zZszaq@"
    "dpg-d486o56r433s739svkj0-a.oregon-postgres.render.com/youtube_oylg"
)


db_conn = None
_background_thread = None


# ---------- DATABASE CONNECTION (FIXED) ----------
def get_db():
    global db_conn
    try:
        if db_conn is None or db_conn.closed:
            raise psycopg.OperationalError("No active connection")
        # Test connection
        with db_conn.cursor() as cur:
            cur.execute("SELECT 1;")
        return db_conn
    except Exception:
        logger.warning("Reconnecting to PostgreSQL...")
        try:
            db_conn = psycopg.connect(
                POSTGRES_URL,
                row_factory=dict_row,
                autocommit=True,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
                sslmode="require"
            )
            logger.info("Database reconnected successfully")
            return db_conn
        except Exception as e:
            logger.error(f"DB reconnect failed: {e}")
            raise


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
    logger.info("Tables ready")


# ---------- YOUTUBE HELPERS ----------
def extract_video_id(link):
    parsed = urlparse(link)
    if parsed.hostname in ("youtube.com", "www.youtube.com"):
        return parse_qs(parsed.query).get("v", [None])[0]
    if parsed.hostname == "youtu.be":
        return parsed.path[1:] if len(parsed.path) > 1 else None
    return None


def fetch_video_title(vid):
    if not youtube:
        return "Unknown Video"
    try:
        resp = youtube.videos().list(part="snippet", id=vid).execute()
        return resp["items"][0]["snippet"]["title"][:50] if resp["items"] else "Unknown"
    except:
        return "Unknown"


def fetch_views(ids):
    if not youtube or not ids:
        return {}
    try:
        resp = youtube.videos().list(part="statistics", id=",".join(ids)).execute()
        return {
            item["id"]: {
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0))
            } for item in resp.get("items", [])
        }
    except Exception as e:
        logger.error(f"API error: {e}")
        return {}


# ---------- SAFE INSERT ----------
def safe_store(vid, stats):
    cur = get_db().cursor()
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")

    cur.execute("DELETE FROM views WHERE video_id=%s AND timestamp=%s", (vid, ts))
    cur.execute("""
        INSERT INTO views (video_id, date, timestamp, views, likes)
        VALUES (%s, %s, %s, %s, %s)
    """, (vid, date, ts, stats["views"], stats["likes"]))
    logger.info(f"STORED {vid} → {stats['views']:,} views")


# ---------- BACKGROUND THREAD ----------
def start_background():
    global _background_thread
    if _background_thread:
        return

    def run():
        while True:
            try:
                conn = get_db()
                cur = conn.cursor()

                now = datetime.now(pytz.timezone("Asia/Kolkata"))
                wait = 300 - (now.minute % 5 * 60 + now.second)
                if wait <= 0:
                    wait += 300
                time.sleep(wait)

                cur.execute("SELECT video_id FROM video_list WHERE is_tracking=1")
                ids = [r["video_id"] for r in cur.fetchall()]
                if ids:
                    stats = {}
                    # Fetch in chunks (50 IDs max per API call)
                    for chunk in [ids[i:i+50] for i in range(0, len(ids), 50)]:
                        stats.update(fetch_views(chunk))
                    for vid in ids:
                        if vid in stats:
                            safe_store(vid, stats[vid])
            except psycopg.OperationalError as e:
                logger.error(f"DB connection dropped: {e}")
                global db_conn
                db_conn = None
                time.sleep(10)
                continue
            except Exception as e:
                logger.error(f"BG error: {e}")
                time.sleep(60)

    _background_thread = threading.Thread(target=run, daemon=True)
    _background_thread.start()
    logger.info("Background task started")


# ---------- DATA PROCESSING ----------
def process_gains(vid, rows):
    if not rows:
        return []
    result = []
    for i, row in enumerate(rows):
        views = row["views"]
        ts = row["timestamp"]
        date = row["date"]

        # Gain since last poll
        gain = 0
        if i > 0 and rows[i-1]["date"] == date:
            gain = views - rows[i-1]["views"]

        # Hourly gain
        ts_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        one_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        cur = get_db().cursor()
        cur.execute("""
            SELECT views FROM views
            WHERE video_id=%s AND date=%s AND timestamp <= %s
            ORDER BY timestamp DESC LIMIT 1
        """, (vid, date, one_ago))
        prev = cur.fetchone()
        hourly = views - prev["views"] if prev else 0

        result.append((ts, views, gain, hourly))
    return result


# ---------- ROUTES ----------
@app.route("/", methods=["GET"])
def index():
    videos = []
    try:
        cur = get_db().cursor()
        cur.execute("SELECT video_id, name, is_tracking FROM video_list")
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
        logger.error(f"Index error: {e}", exc_info=True)
        return render_template("index.html", videos=[], error_message="Loading...")


@app.route("/add_video", methods=["POST"])
def add_video():
    link = request.form.get("video_link", "").strip()
    if not link:
        flash("Enter YouTube link", "error")
        return redirect(url_for("index"))
    vid = extract_video_id(link)
    if not vid:
        flash("Invalid link", "error")
        return redirect(url_for("index"))
    title = fetch_video_title(vid)
    stats = fetch_views([vid])
    if vid not in stats:
        flash("Can't fetch stats", "error")
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
def toggle(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT is_tracking FROM video_list WHERE video_id=%s", (video_id,))
    current = cur.fetchone()["is_tracking"]
    cur.execute("UPDATE video_list SET is_tracking=%s WHERE video_id=%s", (0 if current else 1, video_id))
    flash("Paused" if current else "Resumed", "success")
    return redirect(url_for("index"))


@app.route("/remove_video/<video_id>")
def remove(video_id):
    cur = get_db().cursor()
    cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
    cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
    flash("Removed", "success")
    return redirect(url_for("index"))


@app.route("/export/<video_id>")
def export(video_id):
    cur = get_db().cursor()
    cur.execute("SELECT name FROM video_list WHERE video_id=%s", (video_id,))
    name = cur.fetchone()["name"]
    cur.execute("SELECT timestamp, views FROM views WHERE video_id=%s ORDER BY timestamp", (video_id,))
    rows = cur.fetchall()
    data = [{"Time": r["timestamp"], "Views": r["views"]} for r in rows]
    df = pd.DataFrame(data)
    fname = "export.xlsx"
    df.to_excel(fname, index=False)
    return send_file(fname, as_attachment=True, download_name=f"{name}_views.xlsx")


# ---------- APP START ----------
init_db()
start_background()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))
