<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>YouTube Live Stats</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.min.js"></script>
    <style>
        body{background:linear-gradient(to right,#f8f9fa,#e9ecef);font-family:'Segoe UI',Tahoma,Geneva,Verdana,sans-serif;color:#333}
        .container{max-width:1400px;margin-top:50px;padding:30px;background:#fff;border-radius:20px;box-shadow:0 10px 25px rgba(0,0,0,.1)}
        .song-section{margin-bottom:60px;border-top:2px solid #dee2e6;padding-top:35px}
        .table-responsive{max-height:450px;overflow-y:auto;border:1px solid #dee2e6;border-radius:10px;background:#f8f9fa}
        .video-thumbnail{border-radius:15px;margin-top:15px;max-width:250px;box-shadow:0 4px 8px rgba(0,0,0,.1);transition:transform .3s}
        .video-thumbnail:hover{transform:scale(1.05)}
        .accordion-button{font-weight:bold;background:#e9ecef;color:#333;transition:background .3s}
        .accordion-button:hover{background:#dee2e6}
        .table th,.table td{vertical-align:middle;text-align:center}
        .table thead th{position:sticky;top:0;background:#343a40;color:#fff;z-index:2}
        .chart-container{position:relative;height:300px;width:100%;margin-top:20px}
        .export-btn{margin-left:auto}
        .no-data{text-align:center;color:#6c757d;font-style:italic;padding:2rem}
        .footer{text-align:center;margin-top:3rem;color:#6c757d;font-size:.9rem}
        @media(max-width:768px){.container{padding:15px}.video-thumbnail{max-width:100%}}
    </style>
</head>
<body>
<div class="container">
    <h1 class="text-center mb-4">YouTube Live Stats</h1>
    <p class="text-center text-muted">Real-time view tracking • Updated every 5 minutes</p>

    {% if not videos %}
        <div class="no-data">No videos are being tracked right now.</div>
    {% endif %}

    {% for video in videos %}
    <div class="song-section">
        <div class="d-flex justify-content-between align-items-center flex-wrap mb-3">
            <div class="d-flex align-items-center">
                <img src="https://img.youtube.com/vi/{{ video.video_id }}/0.jpg"
                     class="video-thumbnail me-3" alt="Thumbnail">
                <h3 class="mb-0">{{ video.name | escape }}</h3>
            </div>
            <a href="/export/{{ video.video_id }}" class="btn btn-outline-success export-btn">Export to Excel</a>
        </div>

        <div class="accordion mt-4" id="accordion_{{ video.video_id }}">
            {% for date, data in video.daily_data.items() %}
            <div class="accordion-item">
                <h2 class="accordion-header">
                    <button class="accordion-button {% if not loop.first %}collapsed{% endif %}"
                            type="button" data-bs-toggle="collapse"
                            data-bs-target="#collapse_{{ video.video_id }}_{{ date }}"
                            aria-expanded="{{ 'true' if loop.first else 'false' }}">
                        {{ date }}
                    </button>
                </h2>
                <div id="collapse_{{ video.video_id }}_{{ date }}"
                     class="accordion-collapse collapse {{ 'show' if loop.first else '' }}"
                     data-bs-parent="#accordion_{{ video.video_id }}">
                    <div class="accordion-body">

                        <!-- Table – 4 columns only -->
                        <div class="table-responsive">
                            <table class="table table-striped table-hover table-bordered">
                                <thead class="table-dark">
                                    <tr>
                                        <th>Timestamp (IST)</th>
                                        <th>Views</th>
                                        <th>View Gain</th>
                                        <th>View Hourly Gain</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {% for ts, views, gain, hourly in data[::-1] %}
                                    <tr>
                                        <td>{{ ts }}</td>
                                        <td>{{ "{:,}".format(views) }}</td>
                                        <td style="color:{{ 'green' if gain>0 else 'red' if gain<0 else 'gray' }};">
                                            {{ gain }}
                                        </td>
                                        <td style="color:{{ 'green' if hourly>0 else 'red' if hourly<0 else 'gray' }};">
                                            {{ hourly }}
                                        </td>
                                    </tr>
                                    {% endfor %}
                                </tbody>
                            </table>
                        </div>

                        <!-- Chart (Views + Gains) -->
                        {% if data %}
                        <div class="chart-container">
                            <canvas id="chart_{{ video.video_id }}_{{ date }}"></canvas>
                        </div>
                        <script>
                            new Chart(document.getElementById('chart_{{ video.video_id }}_{{ date }}'), {
                                type: 'line',
                                data: {
                                    labels: [{% for ts,_,_,_ in data[::-1] %}"{{ ts }}",{% endfor %}],
                                    datasets: [
                                        {label:'Views',data:[{% for _,v,_,_ in data[::-1] %}{{ v }},{% endfor %}],borderColor:'#0d6efd',backgroundColor:'rgba(13,110,253,0.2)',fill:true,tension:.4},
                                        {label:'View Gain',data:[{% for _,_,g,_ in data[::-1] %}{{ g }},{% endfor %}],borderColor:'#28a745',backgroundColor:'rgba(40,167,69,0.2)',fill:true,tension:.4},
                                        {label:'Hourly Gain',data:[{% for _,_,_,h in data[::-1] %}{{ h }},{% endfor %}],borderColor:'#ff851b',backgroundColor:'rgba(255,133,27,0.2)',fill:true,tension:.4}
                                    ]
                                },
                                options: {
                                    responsive:true,maintainAspectRatio:false,
                                    plugins:{legend:{position:'top'},title:{display:true,text:'{{ video.name | safe }} – {{ date }}'}},
                                    scales:{x:{title:{display:true,text:'Timestamp (IST)'}},y:{title:{display:true,text:'Count'},beginAtZero:false}}
                                }
                            });
                        </script>
                        {% endif %}
                    </div>
                </div>
            </div>
            {% endfor %}
        </div>
    </div>
    {% endfor %}

    <div class="footer">
        Live data powered by YouTube API • Updated every 5 minutes
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
