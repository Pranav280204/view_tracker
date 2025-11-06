<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>YouTube Views Tracker</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2"></script>
    <style>
        body { background: #f8f9fa; font-family: 'Segoe UI', sans-serif; }
        .container { max-width: 1450px; margin-top: 40px; }
        .card-video { border-radius: 20px; overflow: hidden; box-shadow: 0 10px 30px rgba(0,0,0,0.1); }
        .thumb { border-radius: 15px; transition: 0.3s; }
        .thumb:hover { transform: scale(1.06); }
        .table thead { background: #343a40; color: white; position: sticky; top: 0; z-index: 2; }
        .gain-positive { color: #28a745; font-weight: bold; }
        .gain-zero { color: #6c757d; }
        .chart-container { height: 320px; }
        .last-updated { font-size: 0.9rem; color: #555; }
    </style>
</head>
<body>
<div class="container">
    <h1 class="text-center mb-5">YouTube Views Tracker</h1>

    {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
            <div class="text-center mb-4">
                {% for category, msg in messages %}
                    <div class="alert alert-{{ 'danger' if category == 'error' else 'success' }} d-inline-block px-4 py-2">
                        {{ msg }}
                    </div>
                {% endfor %}
            </div>
        {% endif %}
    {% endwith %}

    <!-- Add Video -->
    <div class="card mb-5 border-0 shadow">
        <div class="card-body p-4">
            <form method="POST" action="/add_video">
                <div class="row g-3">
                    <div class="col-md-5">
                        <input type="url" name="video_link" class="form-control" placeholder="Main video link" required>
                    </div>
                    <div class="col-md-5">
                        <input type="url" name="comparison_link" class="form-control" placeholder="Comparison video (optional)">
                    </div>
                    <div class="col-md-2">
                        <button class="btn btn-primary w-100">Add Video</button>
                    </div>
                </div>
            </form>
        </div>
    </div>

    <!-- Videos -->
    {% for video in videos %}
    <div class="card-video card mb-5">
        <div class="card-header bg-dark text-white d-flex justify-content-between align-items-center">
            <div class="d-flex align-items-center">
                <img src="https://img.youtube.com/vi/{{ video.video_id }}/0.jpg" class="thumb me-3" width="180" alt="thumb">
                <div>
                    <h4 class="mb-0">{{ video.name }}</h4>
                    <small>{{ video.video_id }}</small>
                    {% if not video.is_tracking %}<span class="badge bg-warning ms-2">Paused</span>{% endif %}
                </div>
            </div>
            <div>
                <a href="/export/{{ video.video_id }}" class="btn btn-success btn-sm">Export</a>
                {% if video.is_tracking %}
                    <a href="/stop_tracking/{{ video.video_id }}" class="btn btn-warning btn-sm">Stop</a>
                {% endif %}
                <a href="/remove_video/{{ video.video_id }}" class="btn btn-danger btn-sm" 
                   onclick="return confirm('Delete forever?')">Remove</a>
            </div>
        </div>

        <div class="card-body">
            <div class="accordion" id="acc_{{ video.video_id }}">
                {% for date, rows in video.daily_data.items() %}
                <div class="accordion-item">
                    <h2 class="accordion-header">
                        <button class="accordion-button {% if not loop.first %}collapsed{% endif %}" 
                                data-bs-toggle="collapse" 
                                data-bs-target="#day_{{ video.video_id }}_{{ date }}">
                            <strong>{{ date }}</strong> 
                            <span class="ms-3 text-muted">({{ rows|length }} records)</span>
                        </button>
                    </h2>
                    <div id="day_{{ video.video_id }}_{{ date }}" 
                         class="accordion-collapse collapse {% if loop.first %}show{% endif %}"
                         data-bs-parent="#acc_{{ video.video_id }}">
                        <div class="accordion-body p-0">
                            <div class="table-responsive">
                                <table class="table table-sm table-hover mb-0">
                                    <thead>
                                        <tr>
                                            <th>Time (IST)</th>
                                            <th>Views</th>
                                            <th>5-min Gain</th>
                                            <th>Hourly Gain</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {% for ts, views, gain5, gainH in rows[::-1] %}
                                        <tr>
                                            <td>{{ ts.strftime('%H:%M') }}</td>
                                            <td>{{ views | int }}</td>
                                            <td class="{{ 'gain-positive' if gain5 > 0 else 'gain-zero' }}">
                                                {{ gain5 | int }}
                                            </td>
                                            <td class="{{ 'gain-positive' if gainH > 0 else 'gain-zero' }}">
                                                {{ gainH | int }}
                                            </td>
                                        </tr>
                                        {% endfor %}
                                    </tbody>
                                </table>
                            </div>

                            <div class="chart-container mt-4">
                                <canvas id="chart_{{ video.video_id }}_{{ date }}"></canvas>
                            </div>
                            <script>
                                const ctx{{ loop.index }} = document.getElementById('chart_{{ video.video_id }}_{{ date }}');
                                new Chart(ctx{{ loop.index }}, {
                                    type: 'line',
                                    data: {
                                        labels: [{% for ts, v, g5, gh in rows[::-1] %}"{{ ts.strftime('%H:%M') }}",{% endfor %}],
                                        datasets: [
                                            {
                                                label: 'Total Views',
                                                data: [{% for ts, v, g5, gh in rows[::-1] %}{{ v }},{% endfor %}],
                                                borderColor: '#0d6efd',
                                                backgroundColor: 'rgba(13,110,253,0.1)',
                                                tension: 0.4,
                                                fill: true
                                            },
                                            {
                                                label: '5-min Gain',
                                                data: [{% for ts, v, g5, gh in rows[::-1] %}{{ g5 }},{% endfor %}],
                                                borderColor: '#28a745',
                                                backgroundColor: 'rgba(40,167,69,0.1)',
                                                tension: 0.4,
                                                fill: true
                                            },
                                            {
                                                label: 'Hourly Gain',
                                                data: [{% for ts, v, g5, gh in rows[::-1] %}{{ gh }},{% endfor %}],
                                                borderColor: '#fd7e14',
                                                backgroundColor: 'rgba(253,126,20,0.1)',
                                                tension: 0.4,
                                                fill: true
                                            }
                                        ]
                                    },
                                    options: {
                                        responsive: true,
                                        maintainAspectRatio: false,
                                        plugins: { legend: { position: 'top' } },
                                        scales: { y: { beginAtZero: false } }
                                    }
                                });
                            </script>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
    </div>
    {% else %}
    <div class="text-center text-muted mt-5">
        <h3>No videos yet</h3>
        <p>Add a YouTube link above to start tracking!</p>
    </div>
    {% endfor %}

    <div class="text-center mt-4 last-updated">
        Last updated: <span id="clock"></span> IST
        <script>
            setInterval(() => {
                const now = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
                document.getElementById('clock').innerText = now.split(',')[1].trim();
            }, 1000);
        </script>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
