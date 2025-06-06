import os
import sqlite3
import threading
import time
from datetime import datetime
import pandas as pd
from flask import Flask, render_template, request, send_file
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

app = Flask(__name__)

# YouTube API setup
API_KEY = os.getenv("YOUTUBE_API_KEY")  # Set in Render environment variables
youtube = build("youtube", "v3", developerKey=API_KEY)

# SQLite database setup
def init_db():
    conn = sqlite3.connect("views.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS views (
        video_id TEXT,
        timestamp TEXT,
        views INTEGER
    )""")
    conn.commit()
    conn.close()

# Fetch views for multiple video IDs
def fetch_views(video_ids):
    try:
        response = youtube.videos().list(part="statistics", id=",".join(video_ids)).execute()
        views = {}
        for item in response.get("items", []):
            video_id = item["id"]
            views[video_id] = int(item["statistics"]["viewCount"])
        return views
    except HttpError as e:
        print(f"Error fetching views: {e}")
        return {}

# Store views in database
def store_views(video_id, views):
    conn = sqlite3.connect("views.db")
    c = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO views (video_id, timestamp, views) VALUES (?, ?, ?)",
              (video_id, timestamp, views))
    conn.commit()
    conn.close()

# Background task to fetch views every minute
def fetch_views_periodically():
    pathaan_video_id = "YxWlaYCA8MU"  # Jhoome Jo Pathaan
    default_joshi_video_id = "UCR5C2a0pv_5S-0a8pV2y1jg"  # Sourav Joshi default video
    while True:
        # Get the latest changeable video ID from the database
        conn = sqlite3.connect("views.db")
        c = conn.cursor()
        c.execute("SELECT DISTINCT video_id FROM views WHERE video_id != ? ORDER BY timestamp DESC LIMIT 1", (pathaan_video_id,))
        result = c.fetchone()
        changeable_video_id = result[0] if result else default_joshi_video_id
        conn.close()

        # Fetch views for both videos in one API call
        video_ids = [pathaan_video_id, changeable_video_id]
        views_dict = fetch_views(video_ids)
        
        # Store views
        for video_id, views in views_dict.items():
            if views:
                store_views(video_id, views)
        
        time.sleep(60)  # Wait 1 minute

# Start background task
def start_background_task():
    thread = threading.Thread(target=fetch_views_periodically, daemon=True)
    thread.start()

# Route for home page
@app.route("/", methods=["GET", "POST"])
def index():
    pathaan_video_id = "YxWlaYCA8MU"  # Jhoome Jo Pathaan
    changeable_video_id = None
    default_joshi_video_id = "UCR5C2a0pv_5S-0a8pV2y1jg"  # Sourav Joshi default video

    if request.method == "POST":
        changeable_video_id = request.form.get("video_id")
        if changeable_video_id:
            views = fetch_views([changeable_video_id])
            if changeable_video_id in views and views[changeable_video_id]:
                store_views(changeable_video_id, views[changeable_video_id])

    # Fetch data from database
    conn = sqlite3.connect("views.db")
    c = conn.cursor()
    
    # Get views for Jhoome Jo Pathaan
    c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", (pathaan_video_id,))
    pathaan_data = c.fetchall()
    
    # Get views for changeable video (latest or default)
    if not changeable_video_id:
        c.execute("SELECT DISTINCT video_id FROM views WHERE video_id != ? ORDER BY timestamp DESC LIMIT 1", (pathaan_video_id,))
        result = c.fetchone()
        changeable_video_id = result[0] if result else default_joshi_video_id
    
    c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", (changeable_video_id,))
    joshi_data = c.fetchall()
    conn.close()

    # Prepare comparison data
    comparison = []
    for i, (pathaan_time, pathaan_views) in enumerate(pathaan_data):
        pathaan_minute = datetime.strptime(pathaan_time, "%Y-%m-%d %H:%M:%S").replace(second=0)
        joshi_views = 0
        for joshi_time, views in joshi_data:
            joshi_minute = datetime.strptime(joshi_time, "%Y-%m-%d %H:%M:%S").replace(second=0)
            if joshi_minute == pathaan_minute:
                joshi_views = views
                break
        comparison.append({
            "minute": pathaan_minute.strftime("%Y-%m-%d %H:%M"),
            "pathaan_views": pathaan_views,
            "joshi_views": joshi_views,
            "pathaan_gain": pathaan_views - (pathaan_data[i-1][1] if i > 0 else pathaan_views),
            "joshi_gain": joshi_views - (joshi_data[i-1][1] if i > 0 and joshi_views else joshi_views)
        })

    return render_template("index.html", comparison=comparison, changeable_video_id=changeable_video_id)

# Route to export data to Excel
@app.route("/export")
def export():
    conn = sqlite3.connect("views.db")
    c = conn.cursor()
    
    # Fetch all data
    c.execute("SELECT video_id, timestamp, views FROM views ORDER BY timestamp")
    rows = c.fetchall()
    conn.close()
    
    data = []
    for row in rows:
        video_name = "Jhoome Jo Pathaan" if row[0] == "YxWlaYCA8MU" else "Sourav Joshi (or other)"
        data.append({
            "Video": video_name,
            "Timestamp": row[1],
            "Views": row[2]
        })
    
    # Create DataFrame and export to Excel
    df = pd.DataFrame(data)
    excel_file = "youtube_views.xlsx"
    df.to_excel(excel_file, index=False)
    
    return send_file(excel_file, as_attachment=True)

if __name__ == "__main__":
    init_db()
    start_background_task()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
