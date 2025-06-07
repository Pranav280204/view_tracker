import os
import threading
import logging
import pytz
import sqlite3
import time
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, send_file
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
    while True:
        try:
            video_ids = [video_id_1, video_id_2]
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
        # Calculate view gain (difference from previous timestamp)
        view_gain = 0 if i == 0 else views - data[i-1][1]
        processed_data.append((timestamp, views, view_gain))
    return processed_data

# Route for home page
@app.route("/", methods=["GET"])
def index():
    video_id_1 = "hxMNYkLN7tI"  # Aj Ki Raat
    video_id_2 = "ekr2nIex040"  # Rose
    error_message = None

    try:
        init_db()
        
        conn = sqlite3.connect("views.db", check_same_thread=False)
        c = conn.cursor()
        
        # Fetch data for Aj Ki Raat
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp DESC", (video_id_1,))
        aj_ki_raat_data = process_view_gains(c.fetchall())
        
        # Fetch data for Rose
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp DESC", (video_id_2,))
        rose_data = process_view_gains(c.fetchall())
        
        conn.close()

        return render_template(
            "index.html",
            aj_ki_raat_data=aj_ki_raat_data,
            rose_data=rose_data,
            error_message=error_message,
            song1_name="Aj Ki Raat",
            song2_name="Rose"
        )
    
    except sqlite3.Error as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        init_db()
        return render_template(
            "index.html",
            aj_ki_raat_data=[],
            rose_data=[],
            error_message=f"Database error: {e}",
            song1_name="Aj Ki Raat",
            song2_name="Rose"
        )
    except Exception as e:
        logger.error(f"Error in index route: {e}", exc_info=True)
        return render_template(
            "index.html",
            aj_ki_raat_data=[],
            rose_data=[],
            error_message=str(e),
            song1_name="Aj Ki Raat",
            song2_name="Rose"
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
        
        conn.close()
        
        # Prepare data with view gains for Aj Ki Raat
        aj_ki_raat_data = [{"Timestamp": row[0], "Views": row[1], "View Gain": 0 if i == 0 else row[1] - aj_ki_raat_rows[i-1][1]} 
                           for i, row in enumerate(aj_ki_raat_rows)]
        aj_ki_raat_df = pd.DataFrame(aj_ki_raat_data)
        
        # Prepare data with view gains for Rose
        rose_data = [{"Timestamp": row[0], "Views": row[1], "View Gain": 0 if i == 0 else row[1] - rose_rows[i-1][1]} 
                     for i, row in enumerate(rose_rows)]
        rose_df = pd.DataFrame(rose_data)
        
        # Create Excel file with two sheets
        excel_file = "youtube_views.xlsx"
        with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
            aj_ki_raat_df.to_excel(writer, sheet_name="Aj Ki Raat", index=False)
            rose_df.to_excel(writer, sheet_name="Rose", index=False)
        
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
