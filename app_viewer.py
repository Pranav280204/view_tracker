# app_viewer.py
from flask import Flask, render_template, send_file
from contextlib import contextmanager
from datetime import datetime, timedelta
import psycopg
from psycopg.rows import dict_row
import os
import logging
import pandas as pd

# === Configuration ===
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
    "?sslmode=prefer"
)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# === Database Context Manager ===
@contextmanager
def get_db_cursor():
    conn = psycopg.connect(POSTGRES_URL, row_factory=dict_row)
    try:
        yield conn.cursor()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# === Process Data: Only 4 Columns (In-Memory) ===
def process_view_gains(rows):
    if not rows:
        return []
    processed = []
    for i, row in enumerate(rows):
        timestamp = row["timestamp"]
        views = row["views"]
        date = row["date"]

        # View Gain (since last entry on same day)
        view_gain = 0
        if i > 0 and rows[i-1]["date"] == date:
            view_gain = views - rows[i-1]["views"]

        # View Hourly Gain
        view_hourly = 0
        try:
            ts_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            one_hour_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            for prev in reversed(rows[:i]):
                if prev["date"] != date:
                    break
                if prev["timestamp"] <= one_hour_ago:
                    view_hourly = views - prev["views"]
                    break
        except:
            view_hourly = 0

        processed.append((timestamp, views, view_gain, view_hourly))
    return processed

# === Route: Public Viewer ===
@app.route("/")
def viewer():
    videos = []
    try:
        with get_db_cursor() as cur:
            # Get only active videos
            cur.execute("SELECT video_id, name FROM video_list WHERE is_tracking = 1 ORDER BY name")
            for row in cur.fetchall():
                vid = row["video_id"]
                name = row["name"]

                # Get all dates
                cur.execute("SELECT DISTINCT date FROM views WHERE video_id=%s ORDER BY date DESC", (vid,))
                dates = [r["date"] for r in cur.fetchall()]

                daily_data = {}
                for d in dates:
                    cur.execute("""
                        SELECT date, timestamp, views
                        FROM views
                        WHERE video_id=%s AND date=%s
                        ORDER BY timestamp ASC
                    """, (vid, d))
                    rows = cur.fetchall()
                    daily_data[d] = process_view_gains(rows)

                videos.append({
                    "video_id": vid,
                    "name": name,
                    "daily_data": daily_data
                })

        return render_template("viewer.html", videos=videos)

    except Exception as e:
        logging.error(f"Viewer error: {e}")
        return render_template("viewer.html", videos=[], error_message="Service temporarily unavailable.")

# === Export Route (For Users) ===
@app.route("/export/<video_id>")
def export(video_id):
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT name FROM video_list WHERE video_id=%s AND is_tracking=1", (video_id,))
            row = cur.fetchone()
            if not row:
                return "Video not found or not tracked.", 404
            name = row["name"]

            cur.execute("""
                SELECT timestamp, views, date
                FROM views
                WHERE video_id=%s
                ORDER BY date, timestamp
            """, (video_id,))
            rows = cur.fetchall()

        if not rows:
            return "No data available.", 404

        # Process data for export (4 columns)
        processed_data = process_view_gains(rows)
        export_data = []
        for ts, views, gain, hourly in processed_data:
            export_data.append({
                "Timestamp (IST)": ts,
                "Views": views,
                "View Gain": gain,
                "View Hourly Gain": hourly
            })

        df = pd.DataFrame(export_data)
        fname = f"{name.replace(' ', '_')}_stats.xlsx"
        df.to_excel(fname, index=False, engine="openpyxl")
        return send_file(fname, as_attachment=True, download_name=f"{name}_stats.xlsx")

    except Exception as e:
        logging.error(f"Export error: {e}")
        return "Export failed.", 500

# === Run ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
