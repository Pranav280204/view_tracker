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
API_KEY = os.getenv("YOUTUBE_API_KEY")  # Set this in Render environment variables
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

# Fetch views for a video
def fetch_views(video_id):
    try:
        response = youtube.videos().list(part="statistics", id=video_id).execute()
        if response["items"]:
            return int(response["items"][0]["statistics"]["viewCount"])
        return 0
    except HttpError as e:
        print(f"Error fetching views for {video_id}: {e}")
        return 0

# Store views in database
def store_views(video_id, views):
    conn = sqlite3.connect("views.db")
    c = conn.cursor()
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO views (video_id, timestamp, views) VALUES (?, ?, ?)",
              (video_id, timestamp, views))
    conn.commit()
    conn.close()

# Background task to fetch views hourly
def fetch_views_periodically():
    fixed_video_id = "YxWlaYCA8MU"
    while True:
        # Fetch and store views for fixed video
        views = fetch_views(fixed_video_id)
        if views:
            store_views(fixed_video_id, views)
        # Fetch and store views for the last changeable video
        conn = sqlite3.connect("views.db")
        c = conn.cursor()
        c.execute("SELECT DISTINCT video_id FROM views WHERE video_id != ? ORDER BY timestamp DESC LIMIT 1", (fixed_video_id,))
        result = c.fetchone()
        conn.close()
        if result:
            changeable_video_id = result[0]
            views = fetch_views(changeable_video_id)
            if views:
                store_views(changeable_video_id, views)
        time.sleep(3600)  # Wait 1 hour

# Start background task
def start_background_task():
    thread = threading.Thread(target=fetch_views_periodically, daemon=True)
    thread.start()

# Route for home page
@app.route("/", methods=["GET", "POST"])
def index():
    changeable_video_id = None
    if request.method == "POST":
        changeable_video_id = request.form.get("video_id")
        if changeable_video_id:
            views = fetch_views(changeable_video_id)
            if views:
                store_views(changeable_video_id, views)

    # Fetch data from database
    conn = sqlite3.connect("views.db")
    c = conn.cursor()
    fixed_video_id = "YxWlaYCA8MU"
    
    # Get hourly views for fixed video
    c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", (fixed_video_id,))
    fixed_data = c.fetchall()
    
    # Get hourly views for changeable video (latest one if not specified)
    if not changeable_video_id:
        c.execute("SELECT DISTINCT video_id FROM views WHERE video_id != ? ORDER BY timestamp DESC LIMIT 1", (fixed_video_id,))
        result = c.fetchone()
        changeable_video_id = result[0] if result else None
    
    changeable_data = []
    if changeable_video_id:
        c.execute("SELECT timestamp, views FROM views WHERE video_id = ? ORDER BY timestamp", (changeable_video_id,))
        changeable_data = c.fetchall()
    
    conn.close()

    # Prepare comparison data
    comparison = []
    for i, (fixed_time, fixed_views) in enumerate(fixed_data):
        fixed_hour = datetime.strptime(fixed_time, "%Y-%m-%d %H:%M:%S").replace(minute=0, second=0)
        change_views = 0
        for change_time, views in changeable_data:
            change_hour = datetime.strptime(change_time, "%Y-%m-%d %H:%M:%S").replace(minute=0, second=0)
            if change_hour == fixed_hour:
                change_views = views
                break
        comparison.append({
            "hour": fixed_hour.strftime("%Y-%m-%d %H:00"),
            "fixed_views": fixed_views,
            "changeable_views": change_views,
            "fixed_diff": fixed_views - (fixed_data[i-1][1] if i > 0 else 0),
            "changeable_diff": change_views - (changeable_data[i-1][1] if i > 0 and change_views else 0)
        })

    return render_template("index.html", comparison=comparison, changeable_video_id=changeable_video_id)

# Route to export data to Excel
@app.route("/export")
def export():
    conn = sqlite3.connect("views.db")
    fixed_video_id = "YxWlaYCA8MU"
    c = conn.cursor()
    
    # Get latest changeable video ID
    c.execute("SELECT DISTINCT video_id FROM views WHERE video_id != ? ORDER BY timestamp DESC LIMIT 1", (fixed_video_id,))
    result = c.fetchone()
    changeable_video_id = result[0] if result else None
    
    # Fetch data
    data = []
    c.execute("SELECT video_id, timestamp, views FROM views ORDER BY timestamp")
    rows = c.fetchall()
    conn.close()
    
    for row in rows:
        data.append({
            "Video ID": row[0],
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
