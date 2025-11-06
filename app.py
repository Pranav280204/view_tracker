import os
import threading
import logging
import pytz
import time
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
import pandas as pd
from flask import Flask, render_template, send_file, request, redirect, url_for, flash

# ---------- NEW ----------
import psycopg2
from psycopg2.extras import RealDictCursor   # optional – nicer dict rows
# -------------------------

try:
    import psutil
except ImportError:
    psutil = None
    logging.warning("psutil module not found; memory monitoring disabled")

app = Flask(__name__)
app.secret_key = os.urandom(24)  # For flash messages

# ------------------- Logging -------------------
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ------------------- YouTube API -------------------
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY environment variable is not set")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# ------------------- POSTGRESQL -------------------
# Connection string you gave
POSTGRES_URL = (
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
)

# Global pool – one connection reused by the background thread
db_conn = None

def get_db():
    """Return a fresh connection (with autocommit for background thread)."""
    global db_conn
    if db_conn is None or db_conn.closed:
        db_conn = psycopg2.connect(
            POSTGRES_URL,
            cursor_factory=RealDictCursor,   # optional – rows as dicts
            # keep connection alive for long-running background thread
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5,
        )
        db_conn.autocommit = True   # background thread does many small writes
    return db_conn

def init_db():
    """Create tables if they do not exist."""
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
    # Optional indexes for speed
    cur.execute("CREATE INDEX IF NOT EXISTS idx_views_video_date ON views(video_id, date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_views_timestamp ON views(video_id, timestamp);")
    conn.commit()
    logger.info("PostgreSQL tables ready")

# -------------------------------------------------
# (All the helper functions that talk to the DB are now thin wrappers
#  that simply call `get_db().cursor()` – see examples below)
# -------------------------------------------------

def extract_video_id(video_link): ...   # unchanged
def fetch_video_title(video_id): ...   # unchanged
def fetch_views(video_ids): ...        # unchanged
# -------------------------------------------------

def store_views(video_id, stats):
    """Insert a single row into `views`."""
    conn = get_db()
    cur = conn.cursor()
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    date = now.strftime("%Y-%m-%d")

    views = stats.get("views", 0)
    likes = stats.get("likes", 0)

    cur.execute("""
        INSERT INTO views (video_id, date, timestamp, views, likes)
        VALUES (%s, %s, %s, %s, %s)
    """, (video_id, date, timestamp, views, likes))
    logger.debug(f"Stored stats for {video_id}: views={views}, likes={likes}")

# -------------------------------------------------
# Background thread – only the DB calls change
# -------------------------------------------------
def background_tasks():
    last_sourav_check = None
    default_comparison_id = "YxWlaYCA8MU"

    while True:
        try:
            # … memory logging unchanged …

            # ---- Sourav Joshi auto-add (unchanged logic) ----
            # (just replace every `c = db_conn.cursor()` with `c = get_db().cursor()`)

            # ---- 5-minute polling ----
            # ... time calculation unchanged ...

            c = get_db().cursor()
            c.execute("""
                SELECT video_id, comparison_video_id
                FROM video_list
                WHERE is_tracking = 1
            """)
            video_pairs = c.fetchall()

            video_ids = [row['video_id'] for row in video_pairs]
            comparison_ids = [row['comparison_video_id'] for row in video_pairs if row['comparison_video_id']]
            all_ids = list(set(video_ids + comparison_ids))

            if all_ids:
                stats_dict = fetch_views(all_ids)
                for vid in video_ids:
                    if vid in stats_dict:
                        store_views(vid, stats_dict[vid])
                for cid in comparison_ids:
                    if cid in stats_dict:
                        store_views(cid, stats_dict[cid])

        except Exception as e:
            logger.error(f"Background task error: {e}")

# -------------------------------------------------
# Flask routes – replace every `sqlite3.connect(...)` block
# -------------------------------------------------
@app.route("/", methods=["GET"])
def index():
    error_message = None
    videos = []
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT video_id, name, is_tracking, comparison_video_id
            FROM video_list
        """)
        video_list = cur.fetchall()

        for row in video_list:
            vid = row['video_id']
            name = row['name']
            is_tracking = bool(row['is_tracking'])
            comp_id = row['comparison_video_id']

            # get distinct dates
            cur.execute("""
                SELECT DISTINCT date
                FROM views
                WHERE video_id = %s
                ORDER BY date DESC
            """, (vid,))
            dates = [r['date'] for r in cur.fetchall()]

            daily_data = {}
            for d in dates:
                cur.execute("""
                    SELECT date, timestamp, views, likes
                    FROM views
                    WHERE video_id = %s AND date = %s
                    ORDER BY timestamp ASC
                """, (vid, d))
                daily_data[d] = process_view_gains(vid, cur.fetchall(), comp_id)

            videos.append({
                "video_id": vid,
                "name": name,
                "daily_data": daily_data,
                "is_tracking": is_tracking,
                "comparison_video_id": comp_id
            })

        return render_template("index.html", videos=videos, error_message=error_message)

    except Exception as e:
        logger.error(f"Index error: {e}", exc_info=True)
        init_db()                     # try to recover
        return render_template("index.html", videos=[], error_message=str(e))

# -----------------------------------------------------------------
# The rest of the routes (`add_video`, `stop_tracking`, `remove_video`,
# `export`) follow the same pattern:
#   conn = get_db()
#   cur = conn.cursor()
#   cur.execute(..., (params,))
# -----------------------------------------------------------------

# Example – add_video (only DB part shown)
@app.route("/add_video", methods=["POST"])
def add_video():
    # ... validation unchanged ...

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO video_list (video_id, name, is_tracking, comparison_video_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (video_id) DO UPDATE
        SET name = EXCLUDED.name,
            is_tracking = EXCLUDED.is_tracking,
            comparison_video_id = EXCLUDED.comparison_video_id
    """, (video_id, title, 1, comparison_video_id))

    # store initial stats
    stats = fetch_views([video_id] + ([comparison_video_id] if comparison_video_id else []))
    if video_id in stats:
        store_views(video_id, stats[video_id])
    if comparison_video_id and comparison_video_id in stats:
        store_views(comparison_video_id, stats[comparison_video_id])

    flash("Video added successfully.", "success")
    return redirect(url_for("index"))

# -----------------------------------------------------------------
# Export route – same idea, just use %s placeholders
# -----------------------------------------------------------------
@app.route("/export/<video_id>")
def export(video_id):
    # ... same logic, just replace every `c.execute("SELECT …", (…))` with %s
    # and fetch rows with cur.fetchall() (returns dicts thanks to RealDictCursor)
    ...

# -----------------------------------------------------------------
# Startup
# -----------------------------------------------------------------
init_db()
start_background_tasks()

if __name__ == "__main__":
    app.run(debug=True)
