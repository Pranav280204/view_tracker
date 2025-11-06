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
# PostgreSQL v3 (psycopg)
import psycopg
from psycopg.rows import dict_row
try:
    import psutil
except ImportError:
    psutil = None
    logging.warning("psutil not found – memory monitoring disabled")
app = Flask(**name**)
app.secret_key = os.urandom(24)
# Logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(**name**)
# YouTube API
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY not set")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None
# PostgreSQL (Render)
POSTGRES_URL = (
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
            likes BIGINT NOT NULL
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_views_timestamp ON views(video_id, timestamp);")
    conn.commit()
    logger.info("PostgreSQL tables ready")
# Helper functions
def extract_video_id(video_link):
    try:
        parsed = urlparse(video_link)
        if parsed.hostname in ("www.youtube.com", "youtube.com", "youtu.be"):
            if parsed.hostname == "youtu.be":
                return parsed.path[1:] if len(parsed.path) > 1 else None
            return parse_qs(parsed.query).get("v", [None])[0]
        return None
    except Exception as e:
        logger.error(f"Error parsing video link: {e}")
        return None
def fetch_video_title(video_id):
    if not youtube:
        return None
    try:
        resp = youtube.videos().list(part="snippet", id=video_id).execute()
        for item in resp.get("items", []):
            return item["snippet"]["title"][:50]
        return None
    except HttpError as e:
        logger.error(f"Error fetching title for {video_id}: {e}")
        return None
def fetch_views(video_ids):
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
        logger.error(f"Error fetching stats for {video_ids}: {e}")
        return {}
def store_views(video_id, stats):
    conn = get_db()
    cur = conn.cursor()
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")
    cur.execute("""
        INSERT INTO views (video_id, date, timestamp, views, likes)
        VALUES (%s, %s, %s, %s, %s)
    """, (video_id, date, timestamp, stats.get("views", 0), stats.get("likes", 0)))
    logger.debug(f"Stored {video_id}: views={stats.get('views')}, likes={stats.get('likes')}")
# Background task – 5-minute polling only
def background_tasks():
    while True:
        try:
            if psutil:
                p = psutil.Process()
                mem = p.memory_info()
                logger.debug(f"Memory RSS={mem.rss/1024/1024:.2f}MB VMS={mem.vms/1024/1024:.2f}MB")
            # Wait until next 5-minute mark
            now = datetime.now(pytz.timezone("Asia/Kolkata"))
            mins = now.minute % 5
            secs = now.second + now.microsecond / 1_000_000
            wait = (5 - mins) * 60 - secs
            if wait <= 0:
                wait += 300
            logger.debug(f"Sleeping {wait:.1f}s until next 5-min mark")
            time.sleep(wait)
            # Fetch stats
            cur = get_db().cursor()
            cur.execute("""
                SELECT video_id, comparison_video_id
                FROM video_list
                WHERE is_tracking = 1
            """)
            pairs = cur.fetchall()
            video_ids = [r["video_id"] for r in pairs]
            comp_ids = [r["comparison_video_id"] for r in pairs if r["comparison_video_id"]]
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
            logger.error(f"Background task error: {e}")
def start_background_tasks():
    t = threading.Thread(target=background_tasks, daemon=True)
    t.start()
# Data processing
def process_view_gains(video_id, rows, comparison_video_id=None):
    processed = []
    cur = get_db().cursor()
    for i, row in enumerate(rows):
        date, timestamp, views, likes = row["date"], row["timestamp"], row["views"], row["likes"]
        view_gain = 0 if i == 0 or rows[i-1]["date"] != date else views - rows[i-1]["views"]
        like_gain = 0 if i == 0 or rows[i-1]["date"] != date else likes - rows[i-1]["likes"]
        view_like_ratio = round(views / likes, 2) if likes else 0
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
# Routes
@app.route("/", methods=["GET"])
def index():
    videos = []
    try:
        cur = get_db().cursor()
        cur.execute("SELECT video_id, name, is_tracking, comparison_video_id FROM video_list")
        for row in cur.fetchall():
            vid = row["video_id"]
            cur.execute("SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC", (vid,))
            dates = [r["date"] for r in cur.fetchall()]
            daily = {}
            for d in dates:
                cur.execute("""
                    SELECT date, timestamp, views, likes
                    FROM views WHERE video_id=%s AND date=%s
                    ORDER BY timestamp ASC
                """, (vid, d))
                daily[d] = process_view_gains(vid, cur.fetchall(), row["comparison_video_id"])
            videos.append({
                "video_id": vid,
                "name": row["name"],
                "daily_data": daily,
                "is_tracking": bool(row["is_tracking"]),
                "comparison_video_id": row["comparison_video_id"]
            })
        return render_template("index.html", videos=videos, error_message=None)
    except Exception as e:
        logger.error(f"Index error: {e}", exc_info=True)
        init_db()
        return render_template("index.html", videos=[], error_message=str(e))
@app.route("/add_video", methods=["POST"])
def add_video():
    try:
        vlink = request.form.get("video_link")
        clink = request.form.get("comparison_link")
        if not vlink:
            flash("Video link required.", "error")
            return redirect(url_for("index"))
        vid = extract_video_id(vlink)
        if not vid:
            flash("Invalid YouTube link.", "error")
            return redirect(url_for("index"))
        title = fetch_video_title(vid)
        if not title:
            flash("Cannot fetch title – check link or API key.", "error")
            return redirect(url_for("index"))
        comp_id = None
        if clink:
            comp_id = extract_video_id(clink)
            if not comp_id or not fetch_video_title(comp_id):
                flash("Invalid comparison link.", "error")
                return redirect(url_for("index"))
        stats = fetch_views([vid] + ([comp_id] if comp_id else []))
        if vid not in stats:
            flash("Cannot fetch video stats.", "error")
            return redirect(url_for("index"))
        cur = get_db().cursor()
        cur.execute("""
            INSERT INTO video_list (video_id, name, is_tracking, comparison_video_id)
            VALUES (%s, %s, 1, %s)
            ON CONFLICT (video_id) DO UPDATE
            SET name=EXCLUDED.name, is_tracking=1, comparison_video_id=EXCLUDED.comparison_video_id
        """, (vid, title, comp_id))
        store_views(vid, stats[vid])
        if comp_id and comp_id in stats:
            store_views(comp_id, stats[comp_id])
        flash("Video added.", "success")
        return redirect(url_for("index"))
    except Exception as e:
        logger.error(f"Add video error: {e}")
        flash(str(e), "error")
        return redirect(url_for("index"))
@app.route("/stop_tracking/<video_id>")
def stop_tracking(video_id):
    try:
        cur = get_db().cursor()
        cur.execute("UPDATE video_list SET is_tracking=0 WHERE video_id=%s", (video_id,))
        flash("Stopped tracking.", "success")
    except Exception as e:
        logger.error(f"Stop tracking error: {e}")
        flash(str(e), "error")
    return redirect(url_for("index"))
@app.route("/remove_video/<video_id>")
def remove_video(video_id):
    try:
        cur = get_db().cursor()
        cur.execute("SELECT comparison_video_id FROM video_list WHERE video_id=%s", (video_id,))
        comp = cur.fetchone()
        comp_id = comp["comparison_video_id"] if comp else None
        cur.execute("DELETE FROM views WHERE video_id=%s", (video_id,))
        if comp_id:
            cur.execute("DELETE FROM views WHERE video_id=%s", (comp_id,))
        cur.execute("DELETE FROM video_list WHERE video_id=%s", (video_id,))
        flash("Video removed.", "success")
    except Exception as e:
        logger.error(f"Remove error: {e}")
        flash(str(e), "error")
    return redirect(url_for("index"))
@app.route("/export/<video_id>")
def export(video_id):
    try:
        cur = get_db().cursor()
        cur.execute("SELECT name, comparison_video_id FROM video_list WHERE video_id=%s", (video_id,))
        row = cur.fetchone()
        if not row:
            flash("Video not found.", "error")
            return redirect(url_for("index"))
        name, comp_id = row["name"], row["comparison_video_id"]
        cur.execute("""
            SELECT date, timestamp, views, likes
            FROM views WHERE video_id=%s
            ORDER BY date, timestamp
        """, (video_id,))
        rows = cur.fetchall()
        data = []
        for i, r in enumerate(rows):
            date, ts, views, likes = r["date"], r["timestamp"], r["views"], r["likes"]
            view_gain = 0 if i == 0 or rows[i-1]["date"] != date else views - rows[i-1]["views"]
            like_gain = 0 if i == 0 or rows[i-1]["date"] != date else likes - rows[i-1]["likes"]
            vl_ratio = round(views / likes, 2) if likes else 0
            ts_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            one_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            v_hour = l_hour = 0
            cur.execute("""
                SELECT views FROM views
                WHERE video_id=%s AND date=%s AND timestamp<=%s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_ago))
            prev = cur.fetchone()
            if prev:
                v_hour = views - prev["views"]
            cur.execute("""
                SELECT likes FROM views
                WHERE video_id=%s AND date=%s AND timestamp<=%s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_ago))
            prev = cur.fetchone()
            if prev:
                l_hour = likes - prev["likes"]
            comp_ratio = None
            if comp_id:
                cur.execute("""
                    SELECT views FROM views
                    WHERE video_id=%s AND date=%s AND timestamp<=%s
                    ORDER BY timestamp DESC LIMIT 1
                """, (comp_id, date, one_ago))
                p = cur.fetchone()
                cur.execute("""
                    SELECT views FROM views
                    WHERE video_id=%s AND date=%s AND timestamp=%s
                """, (comp_id, date, ts))
                c = cur.fetchone()
                if p and c and (c["views"] - p["views"]) != 0:
                    comp_ratio = round(v_hour / (c["views"] - p["views"]), 2)
            data.append({
                "Date": date, "Timestamp": ts, "Views": views, "Likes": likes,
                "View Gain": view_gain, "Like Gain": like_gain,
                "View Hourly Gain": v_hour, "Like Hourly Gain": l_hour,
                "Views/Likes Ratio": vl_ratio, "Comparison View Ratio": comp_ratio
            })
        df = pd.DataFrame(data)
        fname = f"youtube_stats_{video_id}.xlsx"
        df.to_excel(fname, sheet_name=name[:31], index=False, engine="openpyxl")
        return send_file(fname, as_attachment=True, download_name=f"{name}_stats.xlsx")
    except Exception as e:
        logger.error(f"Export error: {e}", exc_info=True)
        flash("Export failed.", "error")
        return redirect(url_for("index"))
# Startup
init_db()
start_background_tasks()
if **name** == "**main**":
    app.run(debug=True)
