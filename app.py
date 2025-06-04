import sqlite3
import datetime
import os
import json
import re
from flask import Flask, jsonify, render_template, request
from googleapiclient.discovery import build
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import atexit

app = Flask(__name__)

# Configuration file
CONFIG_FILE = "config.json"
DB_FILE = "youtube_views.db"

def is_valid_youtube_id(video_id):
    """Validate YouTube video ID format (11 characters, alphanumeric with -_)."""
    return bool(re.match(r'^[a-zA-Z0-9_-]{11}$', video_id))

def extract_video_id(url_or_id):
    """Extract video ID from YouTube URL or return ID if already valid."""
    if is_valid_youtube_id(url_or_id):
        return url_or_id
    
    # Try to extract from various YouTube URL formats
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/v/([a-zA-Z0-9_-]{11})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url_or_id)
        if match:
            return match.group(1)
    
    return None

def load_config():
    """Load or initialize configuration from config.json."""
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, "r") as f:
                config = json.load(f)
                # Ensure VIDEO_IDS exists
                if "VIDEO_IDS" not in config:
                    config["VIDEO_IDS"] = []
                return config
        else:
            config = {
                "API_KEY": os.getenv("YOUTUBE_API_KEY"),
                "VIDEO_IDS": []
            }
            if not config["API_KEY"]:
                print("Warning: YOUTUBE_API_KEY environment variable is not set")
            with open(CONFIG_FILE, "w") as f:
                json.dump(config, f, indent=2)
            return config
    except (json.JSONDecodeError, IOError) as e:
        print(f"Error loading config: {e}")
        return {"API_KEY": os.getenv("YOUTUBE_API_KEY"), "VIDEO_IDS": []}

def save_config(video_ids):
    """Save video IDs to config.json."""
    try:
        config = {
            "API_KEY": os.getenv("YOUTUBE_API_KEY"),
            "VIDEO_IDS": video_ids
        }
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f, indent=2)
    except IOError as e:
        print(f"Error saving config: {e}")

def init_db():
    """Initialize SQLite database."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS video_views (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    video_id TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    timestamp DATETIME NOT NULL,
                    UNIQUE(video_id, timestamp)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS video_info (
                    video_id TEXT PRIMARY KEY,
                    title TEXT,
                    channel_title TEXT,
                    thumbnail_url TEXT,
                    duration TEXT,
                    published_at TEXT
                )
            """)
            
            # Create index for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_video_timestamp 
                ON video_views(video_id, timestamp DESC)
            """)
            
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error initializing database: {e}")

def get_video_info(youtube, video_ids):
    """Fetch video information including title, channel, etc."""
    try:
        request = youtube.videos().list(
            part="snippet,contentDetails",
            id=','.join(video_ids)
        )
        response = request.execute()
        
        video_info = {}
        for item in response.get("items", []):
            video_id = item["id"]
            snippet = item["snippet"]
            content_details = item["contentDetails"]
            
            video_info[video_id] = {
                'title': snippet.get('title', ''),
                'channel_title': snippet.get('channelTitle', ''),
                'thumbnail_url': snippet.get('thumbnails', {}).get('medium', {}).get('url', ''),
                'duration': content_details.get('duration', ''),
                'published_at': snippet.get('publishedAt', '')
            }
        
        return video_info
    except Exception as e:
        print(f"Error fetching video info: {e}")
        return {}

def save_video_info(video_info):
    """Save video information to database."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            for video_id, info in video_info.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO video_info 
                    (video_id, title, channel_title, thumbnail_url, duration, published_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    video_id,
                    info['title'],
                    info['channel_title'],
                    info['thumbnail_url'],
                    info['duration'],
                    info['published_at']
                ))
            conn.commit()
    except sqlite3.Error as e:
        print(f"Error saving video info: {e}")

def fetch_video_views():
    """Fetch view counts for all video IDs and store in database."""
    config = load_config()
    if not config["API_KEY"]:
        print("No API key configured")
        return
    
    if not config["VIDEO_IDS"]:
        print("No video IDs configured")
        return

    try:
        youtube = build('youtube', 'v3', developerKey=config["API_KEY"])
        
        # Fetch video statistics
        request = youtube.videos().list(
            part="statistics",
            id=','.join(config["VIDEO_IDS"])
        )
        response = request.execute()
        
        # Fetch video info for new videos
        video_info = get_video_info(youtube, config["VIDEO_IDS"])
        save_video_info(video_info)

        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            timestamp = datetime.datetime.now(pytz.UTC)
            
            for item in response.get("items", []):
                video_id = item["id"]
                view_count = int(item["statistics"].get("viewCount", 0))
                
                # Use INSERT OR IGNORE to prevent duplicates
                cursor.execute("""
                    INSERT OR IGNORE INTO video_views (video_id, view_count, timestamp) 
                    VALUES (?, ?, ?)
                """, (video_id, view_count, timestamp.isoformat()))
            
            conn.commit()
            print(f"Successfully updated view counts at {timestamp}")
            
    except Exception as e:
        print(f"Error fetching video views: {e}")

# Initialize database
init_db()

# Initialize scheduler
scheduler = BackgroundScheduler(timezone=pytz.UTC)
scheduler.add_job(
    fetch_video_views,
    trigger=CronTrigger(minute='*/30'),  # Run every 30 minutes
    id='fetch_video_views',
    replace_existing=True
)
scheduler.start()

# Shutdown scheduler on app exit
atexit.register(lambda: scheduler.shutdown())

@app.route('/')
def index():
    """Render the main page with current video IDs."""
    config = load_config()
    return render_template('index.html', video_ids=config['VIDEO_IDS'])

@app.route('/add_video', methods=['POST'])
def add_video():
    """Add a video ID to the configuration."""
    try:
        data = request.get_json()
        video_input = data.get('video_id', '').strip()
        
        if not video_input:
            return jsonify({'success': False, 'message': 'Please provide a video ID or URL'}), 400
        
        video_id = extract_video_id(video_input)
        if not video_id:
            return jsonify({'success': False, 'message': 'Invalid YouTube video ID or URL'}), 400
        
        config = load_config()
        if video_id in config['VIDEO_IDS']:
            return jsonify({'success': False, 'message': 'Video is already being tracked'}), 400
        
        # Verify the video exists by trying to fetch its info
        if config['API_KEY']:
            try:
                youtube = build('youtube', 'v3', developerKey=config['API_KEY'])
                request = youtube.videos().list(part="snippet", id=video_id)
                response = request.execute()
                
                if not response.get('items'):
                    return jsonify({'success': False, 'message': 'Video not found or is private'}), 400
                
            except Exception as e:
                print(f"Error verifying video: {e}")
                return jsonify({'success': False, 'message': 'Error verifying video'}), 500
        
        config['VIDEO_IDS'].append(video_id)
        save_config(config['VIDEO_IDS'])
        
        # Immediately fetch data for the new video
        fetch_video_views()
        
        return jsonify({'success': True, 'message': 'Video added successfully', 'video_id': video_id})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error adding video: {str(e)}'}), 500

@app.route('/remove_video/<video_id>', methods=['POST'])
def remove_video(video_id):
    """Remove a video ID from the configuration."""
    try:
        if not is_valid_youtube_id(video_id):
            return jsonify({'success': False, 'message': 'Invalid YouTube video ID'}), 400
        
        config = load_config()
        if video_id in config['VIDEO_IDS']:
            config['VIDEO_IDS'].remove(video_id)
            save_config(config['VIDEO_IDS'])
            return jsonify({'success': True, 'message': 'Video removed successfully'})
        
        return jsonify({'success': False, 'message': 'Video not found'}), 404
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error removing video: {str(e)}'}), 500

@app.route('/video_data/<video_id>')
def video_data(video_id):
    """Get view count data for a specific video ID."""
    try:
        if not is_valid_youtube_id(video_id):
            return jsonify({'success': False, 'message': 'Invalid YouTube video ID'}), 400
        
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            # Get video info
            cursor.execute("""
                SELECT title, channel_title, thumbnail_url 
                FROM video_info WHERE video_id = ?
            """, (video_id,))
            
            video_info_row = cursor.fetchone()
            video_info = {}
            if video_info_row:
                video_info = {
                    'title': video_info_row[0],
                    'channel_title': video_info_row[1],
                    'thumbnail_url': video_info_row[2]
                }
            
            # Get view count data (last 48 data points)
            cursor.execute("""
                SELECT timestamp, view_count 
                FROM video_views 
                WHERE video_id = ? 
                ORDER BY timestamp DESC 
                LIMIT 48
            """, (video_id,))
            
            rows = cursor.fetchall()
            data = []
            
            for i, row in enumerate(reversed(rows)):  # Reverse to get chronological order
                timestamp, view_count = row
                
                # Calculate view gain
                view_gain = 0
                if i > 0:
                    prev_count = data[i-1]['view_count']
                    view_gain = view_count - prev_count
                
                data.append({
                    'timestamp': timestamp,
                    'view_count': view_count,
                    'view_gain': view_gain
                })
            
        return jsonify({
            'success': True, 
            'data': data,
            'video_info': video_info
        })
        
    except sqlite3.Error as e:
        return jsonify({'success': False, 'message': f'Database error: {str(e)}'}), 500

@app.route('/dashboard_data')
def dashboard_data():
    """Get dashboard data for all tracked videos."""
    try:
        config = load_config()
        dashboard_data = []
        
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            for video_id in config['VIDEO_IDS']:
                # Get latest view count and calculate gains
                cursor.execute("""
                    SELECT view_count, timestamp 
                    FROM video_views 
                    WHERE video_id = ? 
                    ORDER BY timestamp DESC 
                    LIMIT 2
                """, (video_id,))
                
                view_data = cursor.fetchall()
                
                # Get video info
                cursor.execute("""
                    SELECT title, channel_title, thumbnail_url 
                    FROM video_info WHERE video_id = ?
                """, (video_id,))
                
                info_row = cursor.fetchone()
                
                if view_data and info_row:
                    current_views = view_data[0][0]
                    latest_gain = 0
                    
                    if len(view_data) > 1:
                        latest_gain = current_views - view_data[1][0]
                    
                    # Calculate 24-hour gain
                    cursor.execute("""
                        SELECT view_count 
                        FROM video_views 
                        WHERE video_id = ? AND timestamp <= datetime('now', '-24 hours')
                        ORDER BY timestamp DESC 
                        LIMIT 1
                    """, (video_id,))
                    
                    day_ago_data = cursor.fetchone()
                    daily_gain = 0
                    if day_ago_data:
                        daily_gain = current_views - day_ago_data[0]
                    
                    dashboard_data.append({
                        'video_id': video_id,
                        'title': info_row[0],
                        'channel_title': info_row[1],
                        'thumbnail_url': info_row[2],
                        'current_views': current_views,
                        'latest_gain': latest_gain,
                        'daily_gain': daily_gain,
                        'last_updated': view_data[0][1]
                    })
        
        return jsonify({'success': True, 'data': dashboard_data})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error getting dashboard data: {str(e)}'}), 500

@app.route('/trigger_update', methods=['POST'])
def trigger_update():
    """Manually trigger view count update."""
    try:
        fetch_video_views()
        return jsonify({'success': True, 'message': 'View counts updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error updating view counts: {str(e)}'}), 500

if __name__ == '__main__':
    # Run initial fetch
    fetch_video_views()
    app.run(debug=True, host='0.0.0.0', port=5000)
