# app_viewer.py
from flask import Flask, render_template
from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row

POSTGRES_URL = (
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
    "?sslmode=prefer"
)

app = Flask(__name__)

@contextmanager
def get_db_cursor():
    conn = psycopg.connect(POSTGRES_URL, row_factory=dict_row)
    try:
        yield conn.cursor()
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def process_view_gains(video_id, rows, comparison_video_id=None):
    processed = []
    with get_db_cursor() as cur:
        for i, row in enumerate(rows):
            date, timestamp, views, likes = row["date"], row["timestamp"], row["views"], row["likes"]
            view_gain = 0 if i == 0 or rows[i-1]["date"] != date else views - rows[i-1]["views"]
            like_gain = 0 if i == 0 or rows[i-1]["date"] != date else likes - rows[i-1]["likes"]
            view_like_ratio = round(views / likes, 2) if likes else 0

            ts_dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
            one_hour_ago = (ts_dt - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
            view_hourly = like_hourly = 0

            cur.execute("""
                SELECT views FROM views WHERE video_id=%s AND date=%s AND timestamp<=%s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            r = cur.fetchone()
            if r:
                view_hourly = views - r["views"]

            cur.execute("""
                SELECT likes FROM views WHERE video_id=%s AND date=%s AND timestamp<=%s
                ORDER BY timestamp DESC LIMIT 1
            """, (video_id, date, one_hour_ago))
            r = cur.fetchone()
            if r:
                like_hourly = likes - r["likes"]

            comp_ratio = None
            if comparison_video_id:
                cur.execute("""
                    SELECT views FROM views WHERE video_id=%s AND date=%s AND timestamp<=%s
                    ORDER BY timestamp DESC LIMIT 1
                """, (comparison_video_id, date, one_hour_ago))
                prev = cur.fetchone()
                cur.execute("""
                    SELECT views FROM views WHERE video_id=%s AND date=%s AND timestamp=%s
                """, (comparison_video_id, date, timestamp))
                curr = cur.fetchone()
                if prev and curr and (curr["views"] - prev["views"]) != 0:
                    comp_ratio = round(view_hourly / (curr["views"] - prev["views"]), 2)

            processed.append((
                timestamp, views, likes, view_gain, like_gain,
                view_hourly, view_like_ratio, like_hourly, comp_ratio
            ))
    return processed

@app.route("/")
def viewer():
    videos = []
    try:
        with get_db_cursor() as cur:
            cur.execute("SELECT video_id, name, comparison_video_id FROM video_list WHERE is_tracking = 1")
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
                    "daily_data": daily
                })

        return render_template("viewer.html", videos=videos)

    except Exception as e:
        return render_template("viewer.html", videos=[], error_message="Service unavailable")

if __name__ == "__main__":
    app.run()
