import os
import threading
import logging
import pytz
import sqlite3
import time
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, send_file, request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# YouTube API setup
API_KEY = os.getenv("YOUTUBE_API_KEY")
if not API_KEY:
    logger.error("YOUTUBE_API_KEY environment variable is not set")
youtube = build("youtube", "v3", developerKey=API_KEY) if API_KEY else None

# SQLite database setup
def init_db():
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS views (
            video_id TEXT,
            timestamp TEXT,
            views INTEGER
        )""")
        conn.commit()
        logger.info("Successfully initialized database")
    except sqlite3.Error as e:
        logger.error(f"Database initialization failed: {e}")
    finally:
        conn.close()

# Fetch views for multiple video IDs
def fetch_views(video_ids):
    if not youtube:
        logger.error("YouTube API client not initialized")
        return {}
    try:
        response = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        views = {}
        for item in response.get("items", []):
            video_id = item["id"]
            views[video_id] = int(item["statistics"]["viewCount"])
        return views
    except HttpError as e:
        logger.error(f"Error fetching views for {video_ids}: {e}")
        return {}

# Store views in database with IST timestamp
def store_views(video_id, views):
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        ist = pytz.timezone("Asia/Kolkata")
        timestamp = datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT INTO views (video_id, timestamp, views) VALUES (?, ?, ?)",
                  (video_id, timestamp, views))
        conn.commit()
        logger.debug(f"Stored views for {video_id}: {views} at {timestamp} IST")
    except sqlite3.Error as e:
        logger.error(f"Error storing views for {video_id}: {e}")
    finally:
        conn.close()

# Background task to fetch views every 5 minutes
def fetch_views_periodically():
    video_id_1 = "hxMNYkLN7tI"  # Aj Ki Raat
    video_id_2 = "ekr2nIex040"  # Rose
    video_id_3 = "hTSaweR8qMI"  # Keeping the same video ID, renaming to "MrBeast"
    while True:
        try:
            video_ids = [video_id_1, video_id_2, video_id_3]
            views_dict = fetch_views(video_ids)
            
            for video_id, views in views_dict.items():
                if views:
                    store_views(video_id, views)
        except Exception as e:
            logger.error(f"Background task error: {e}")
        time.sleep(300)  # Wait 5 minutes

# Start background task
def start_background_task():
    thread = threading.Thread(target=fetch_views_periodically, daemon=True)
    thread.start()

# Process data to include view gains
def process_view_gains(data):
    processed_data = []
    for i in range(len(data)):
        timestamp, views = data[i]
        view_gain = 0 if i == 0 else views - data[i-1][1]
        processed_data.append((timestamp, views, view_gain))
    return processed_data

# Calculate required views per 5-minute interval to reach target
def calculate_required_views_per_interval(latest_views, target_views, target_time_str, current_time):
    try:
        # Parse target time (format: "YYYY-MM-DD HH:MM:SS")
        target_time = datetime.strptime(target_time_str, "%Y-%m-%d %H:%M:%S")
        target_time = pytz.timezone("Asia/Kolkata").localize(target_time)

        # Current time is already timezone-aware, no need to localize again
        time_diff_seconds = (target_time - current_time).total_seconds()
        if time_diff_seconds <= 0:
            return None, "Target time must be in the future."

        # Number of 5-minute intervals remaining
        intervals_remaining = time_diff_seconds / 300  # 300 seconds = 5 minutes
        if intervals_remaining < 1:
            return None, "Target time is too close (less than 5 minutes)."

        # Calculate views needed to reach target
        views_needed = target_views - latest_views
        if views_needed <= 0:
            return None, "Target views already achieved or invalid."

        # Calculate required views per 5-minute interval
        required_views_per_interval = views_needed / intervals_remaining
        return required_views_per_interval, None
    except ValueError as e:
        return None, f"Invalid target time format: {e}"

# Route for home page
@app.route("/", methods=["GET", "POST"])
def index():
    video_id_1 = "hxMNYkLN7tI"  # Aj Ki Raat
    video_id_2 = "ekr2nIex040"  # Rose
    video_id_3 = "hTSaweR8qMI"  # Keeping the same video ID, renaming to "MrBeast"
    error_message = None
    target_message = None
    required_views_per_interval = None

    try:
        init_db()
        
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        
        # Fetch data for Aj Ki Raat
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp ASC", (video_id_1,))
        aj_ki_raat_data = process_view_gains(c.fetchall())
        
        # Fetch data for Rose
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp ASC", (video_id_2,))
        rose_data = process_view_gains(c.fetchall())
        
        # Fetch data for the renamed video (MrBeast)
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp ASC", (video_id_3,))
        new_video_data = process_view_gains(c.fetchall())
        
        # Handle target views and time for the renamed video
        if request.method == "POST":
            target_views = request.form.get("target_views", type=int)
            target_time = request.form.get("target_time")
            if target_views and target_time and new_video_data:
                latest_views = new_video_data[-1][1]  # Most recent views
                current_time = datetime.now(pytz.timezone("Asia/Kolkata"))
                required_views_per_interval, target_message = calculate_required_views_per_interval(
                    latest_views, target_views, target_time, current_time
                )
        
        conn.close()

        return render_template(
            "index.html",
            aj_ki_raat_data=aj_ki_raat_data,
            rose_data=rose_data,
            new_video_data=new_video_data,
            error_message=error_message,
            target_message=target_message,
            required_views_per_interval=required_views_per_interval,
            song1_name="Aj Ki Raat",
            song2_name="Rose",
            song3_name="MrBeast"  # Changed to "MrBeast" as requested
        )
    
    except sqlite3.Error as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        init_db()
        return render_template(
            "index.html",
            aj_ki_raat_data=[],
            rose_data=[],
            new_video_data=[],
            error_message=f"Database error: {e}",
            target_message=None,
            required_views_per_interval=None,
            song1_name="Aj Ki Raat",
            song2_name="Rose",
            song3_name="MrBeast"
        )
    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        return render_template(
            "index.html",
            aj_ki_raat_data=[],
            rose_data=[],
            new_video_data=[],
            error_message=str(e),
            target_message=None,
            required_views_per_interval=None,
            song1_name="Aj Ki Raat",
            song2_name="Rose",
            song3_name="MrBeast"
        )

# Route to export to Excel
@app.route("/export")
def export():
    try:
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        
        # Fetch data for Aj Ki Raat
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", ("hxMNYkLN7tI",))
        aj_ki_raat_rows = c.fetchall()
        
        # Fetch data for Rose
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", ("ekr2nIex040",))
        rose_rows = c.fetchall()
        
        # Fetch data for the renamed video (MrBeast)
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", ("hTSaweR8qMI",))
        new_video_rows = c.fetchall()
        
        conn.close()
        
        # Prepare data with view gains for Aj Ki Raat
        aj_ki_raat_data = [{"Timestamp": row[0], "Views": row[1], "View Gain": 0 if i == 0 else row[1] - aj_ki_raat_rows[i-1][1]} 
                           for i, row in enumerate(aj_ki_raat_rows)]
        aj_ki_raat_df = pd.DataFrame(aj_ki_raat_data)
        
        # Prepare data with view gains for Rose
        rose_data = [{"Timestamp": row[0], "Views": row[1], "View Gain": 0 if i == 0 else row[1] - rose_rows[i-1][1]} 
                     for i, row in enumerate(rose_rows)]
        rose_df = pd.DataFrame(rose_data)
        
        # Prepare data with view gains for the renamed video (MrBeast)
        new_video_data = [{"Timestamp": row[0], "Views": row[1], "View Gain": 0 if i == 0 else row[1] - new_video_rows[i-1][1]} 
                          for i, row in enumerate(new_video_rows)]
        new_video_df = pd.DataFrame(new_video_data)
        
        # Create Excel file with three sheets
        excel_file = "youtube_views.xlsx"
        with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
            aj_ki_raat_df.to_excel(writer, sheet_name="Aj Ki Raat", index=False)
            rose_df.to_excel(writer, sheet_name="Rose", index=False)
            new_video_df.to_excel(writer, sheet_name="MrBeast", index=False)  # Updated sheet name to "MrBeast"
        
        return send_file(excel_file, as_attachment=True)
    except sqlite3.Error as e:
        logger.error(f"Database error in export route: {e}", exc_info=True)
        return "Database error exporting data", 500
    except Exception as e:
        logger.error(f"Error in export route: {e}", exc_info=True)
        return "Error exporting data", 500

# Initialize database and start background task at app startup
init_db()
start_background_task()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
