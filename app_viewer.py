# app_viewer.py
from flask import Flask, render_template
import psycopg
from psycopg.rows import dict_row
import pandas as pd
import plotly.express as px
import plotly.utils
import json

POSTGRES_URL = (
    "postgresql://ytanalysis_db_user:Uqy7UPp7lOfu1sEHvVOKlWwozrhpZzCk@"
    "dpg-d46am6q4d50c73cgrkv0-a.oregon-postgres.render.com/ytanalysis_db"
    "?sslmode=prefer"
)

app = Flask(__name__)

def get_db():
    return psycopg.connect(POSTGRES_URL, row_factory=dict_row)

@app.route("/")
def viewer():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT v.video_id, vl.name, v.date, v.timestamp, v.views, v.likes
                    FROM views v
                    JOIN video_list vl ON v.video_id = vl.video_id
                    WHERE vl.is_tracking = 1
                    ORDER BY v.timestamp DESC
                    LIMIT 500
                """)
                rows = cur.fetchall()

        if not rows:
            return render_template("viewer.html", graphs=[], message="No data yet.")

        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        graphs = []
        for video_id in df['video_id'].unique():
            name = df[df['video_id'] == video_id]['name'].iloc[0]
            video_df = df[df['video_id'] == video_id].sort_values('timestamp')

            # Views over time
            fig1 = px.line(video_df, x='timestamp', y='views', title=f"{name} – Views")
            fig1.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
            graph1 = json.dumps(fig1, cls=plotly.utils.PlotlyJSONEncoder)

            # Likes over time
            fig2 = px.line(video_df, x='timestamp', y='likes', title=f"{name} – Likes", color_discrete_sequence=['#ff6b6b'])
            fig2.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20))
            graph2 = json.dumps(fig2, cls=plotly.utils.PlotlyJSONEncoder)

            graphs.append({
                "video_id": video_id,
                "name": name,
                "views_graph": graph1,
                "likes_graph": graph2,
                "latest_views": int(video_df['views'].iloc[-1]),
                "latest_likes": int(video_df['likes'].iloc[-1]),
                "hourly_gain": int(video_df['views'].iloc[-1] - video_df['views'].iloc[-13] if len(video_df) > 12 else 0)
            })

        return render_template("viewer.html", graphs=graphs, message=None)

    except Exception as e:
        return render_template("viewer.html", graphs=[], message="Service temporarily unavailable.")

if __name__ == "__main__":
    app.run()
