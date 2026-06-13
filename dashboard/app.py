from flask import Flask, jsonify, render_template_string
import subprocess
import json
from datetime import datetime

app = Flask(__name__)

def read_hdfs_json(hdfs_path):
    try:
        result = subprocess.run(
            ["docker", "exec", "aira-namenode", "hdfs", "dfs", "-ls", hdfs_path],
            capture_output=True, text=True, check=True
        )
        files = []
        for line in result.stdout.strip().split('\n'):
            if '.json' in line:
                parts = line.split()
                files.append(parts[-1])
        
        records = []
        for f in files:
            cat_result = subprocess.run(
                ["docker", "exec", "aira-namenode", "hdfs", "dfs", "-cat", f],
                capture_output=True, text=True, check=True
            )
            for line in cat_result.stdout.strip().split('\n'):
                if line:
                    records.append(json.loads(line))
        return records
    except Exception as e:
        print(f"[ERROR] {e}")
        return []

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>AIRA — Jakarta Air Quality Dashboard</title>
    
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    
    <style>
        :root {
            --bg-main: #0b1120;
            --bg-card: rgba(30, 41, 59, 0.6);
            --border-color: rgba(255, 255, 255, 0.08);
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --accent: #38bdf8;
        }

        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Inter', sans-serif; background: var(--bg-main); color: var(--text-main); overflow-x: hidden; }

        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: var(--bg-main); }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }

        header {
            background: rgba(15, 23, 42, 0.8);
            backdrop-filter: blur(12px);
            padding: 20px 40px;
            border-bottom: 1px solid var(--border-color);
            display: flex;
            justify-content: space-between;
            align-items: center;
            position: sticky;
            top: 0;
            z-index: 1000;
        }
        header h1 { font-size: 1.6rem; color: var(--accent); font-weight: 600; letter-spacing: -0.5px; }
        header h1 span { color: var(--text-muted); font-weight: 400; font-size: 1rem; }
        #last-update { font-size: 0.8rem; color: var(--text-muted); margin-top: 6px; font-weight: 500; }
        
        .refresh-btn {
            background: linear-gradient(135deg, #0ea5e9, #2563eb);
            color: white; border: none; padding: 10px 24px;
            border-radius: 8px; cursor: pointer; font-size: 0.9rem; font-weight: 500;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(37, 99, 235, 0.2);
        }
        .refresh-btn:hover { transform: translateY(-2px); box-shadow: 0 6px 16px rgba(37, 99, 235, 0.4); }

        .container { padding: 32px 40px; max-width: 1600px; margin: 0 auto; }

        .section-title {
            font-size: 0.8rem; color: var(--text-muted); text-transform: uppercase;
            letter-spacing: 2px; margin-bottom: 16px; margin-top: 32px; font-weight: 600;
            display: flex; align-items: center; gap: 8px;
        }

        .card-base {
            background: var(--bg-card);
            backdrop-filter: blur(16px);
            border-radius: 16px;
            padding: 24px;
            border: 1px solid var(--border-color);
            box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1);
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .card-base:hover {
            transform: translateY(-4px);
            box-shadow: 0 10px 15px -3px rgba(0,0,0,0.2);
            border-color: rgba(255,255,255,0.15);
        }

        .cards, .weather-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
        }

        .card .station, .weather-card .w-station {
            font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase;
            letter-spacing: 1px; font-weight: 600;
        }
        .card .aqi-value { font-size: 2.8rem; font-weight: 700; margin: 8px 0; line-height: 1; }
        .card .aqi-cat { font-size: 0.75rem; padding: 4px 12px; border-radius: 20px; font-weight: 600; display: inline-block; }
        .card .meta, .weather-card .w-meta { font-size: 0.8rem; color: var(--text-muted); margin-top: 12px; line-height: 1.6; }

        .weather-card .w-temp { font-size: 2.2rem; font-weight: 700; color: #fb923c; margin: 8px 0; line-height: 1; }
        .weather-card .w-desc { font-size: 0.85rem; color: var(--accent); font-weight: 500; margin-bottom: 12px; }

        .info-row {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
        }

        .traffic-box h3, .uv-box h3, .chart-box h3, .map-box h3, .table-box h3 {
            font-size: 0.85rem; color: var(--text-main); text-transform: uppercase;
            letter-spacing: 1px; margin-bottom: 20px; font-weight: 600;
            border-bottom: 1px solid var(--border-color); padding-bottom: 12px;
        }

        .traffic-item {
            display: flex; justify-content: space-between; align-items: center;
            padding: 12px 0; border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .traffic-item:last-child { border-bottom: none; }
        .traffic-point { font-size: 0.9rem; font-weight: 500; }
        .traffic-speed { font-size: 0.8rem; color: var(--text-muted); margin-top: 4px; }
        .traffic-right { text-align: right; }
        .congestion-bar-bg {
            height: 6px; background: rgba(0,0,0,0.3); border-radius: 4px;
            margin-top: 6px; width: 120px; overflow: hidden;
        }
        .congestion-bar { height: 100%; border-radius: 4px; transition: width 0.5s ease-in-out; }

        .uv-big { font-size: 4rem; font-weight: 700; line-height: 1; }
        .uv-label { font-size: 1rem; color: var(--text-muted); margin-top: 8px; font-weight: 500; }
        .uv-meta { font-size: 0.85rem; color: var(--text-muted); margin-top: 20px; line-height: 1.8; }

        .charts {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
            gap: 20px;
        }
        .chart-box canvas { max-height: 300px; }
        .full-width { grid-column: 1 / -1; }

        #map { height: 420px; border-radius: 12px; z-index: 1; }

        .table-wrapper { overflow-x: auto; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th { text-align: left; padding: 12px 16px; font-size: 0.75rem; color: var(--text-muted); border-bottom: 1px solid var(--border-color); text-transform: uppercase; letter-spacing: 1px; }
        td { padding: 14px 16px; font-size: 0.85rem; border-bottom: 1px solid rgba(255,255,255,0.03); font-weight: 500; }
        tr:hover td { background: rgba(255,255,255,0.03); }

        .good        { color: #4ade80; }
        .moderate    { color: #facc15; }
        .sensitive   { color: #fb923c; }
        .unhealthy   { color: #f87171; }
        .very-bad    { color: #c084fc; }

        .badge-good      { background: rgba(74,222,128,0.15);  color: #4ade80; border: 1px solid rgba(74,222,128,0.3);  }
        .badge-moderate  { background: rgba(250,204,21,0.15);  color: #facc15; border: 1px solid rgba(250,204,21,0.3);  }
        .badge-sensitive { background: rgba(251,146,60,0.15);  color: #fb923c; border: 1px solid rgba(251,146,60,0.3);  }
        .badge-unhealthy { background: rgba(248,113,113,0.15); color: #f87171; border: 1px solid rgba(248,113,113,0.3); }
        .badge-very-bad  { background: rgba(192,132,252,0.15); color: #c084fc; border: 1px solid rgba(192,132,252,0.3); }

        @media (max-width: 768px) {
            .info-row, .charts { grid-template-columns: 1fr; }
            header { flex-direction: column; gap: 16px; align-items: flex-start; }
            .container { padding: 20px; }
        }
    </style>
</head>
<body>
<header>
    <div>
        <h1>AIRA <span>— Jakarta Air Quality Intelligence</span></h1>
        <div id="last-update">Last update: loading...</div>
    </div>
    <button class="refresh-btn" onclick="refreshAll()">⟳ Refresh Data</button>
</header>

<div class="container">

    <!-- AQI CARDS -->
    <div class="section-title">🌫️ Air Quality Index — Per Stasiun</div>
    <div class="cards" id="station-cards"></div>

    <!-- WEATHER CARDS -->
    <div class="section-title">🌤️ Kondisi Cuaca Real-time</div>
    <div class="weather-cards" id="weather-cards"></div>

    <!-- TRAFFIC + UV -->
    <div class="section-title">🚗 Lalu Lintas & ☀️ Indeks UV</div>
    <div class="info-row">
        <div class="card-base traffic-box">
            <h3>Kemacetan Koridor Utama Jakarta</h3>
            <div id="traffic-list"></div>
        </div>
        <div class="card-base uv-box">
            <h3>Indeks UV & Ozon</h3>
            <div id="uv-info"></div>
        </div>
    </div>

    <!-- ML PREDICTIONS -->
    <div class="section-title">🤖 Machine Learning — Random Forest (R²=0.98)</div>
    <div class="charts" style="margin-bottom:20px">
        <div class="card-base chart-box">
            <h3>AQI Aktual vs ML Prediction</h3>
            <canvas id="mlPredChart"></canvas>
        </div>
    </div>
    <div class="info-row" style="margin-bottom:20px">
        <div class="card-base table-box">
            <h3>Detail ML Predictions per Stasiun</h3>
            <div class="table-wrapper">
                <table>
                    <thead>
                        <tr>
                            <th>Stasiun</th>
                            <th>AQI Aktual</th>
                            <th>ML Prediction</th>
                            <th>Kategori</th>
                            <th>Delta</th>
                        </tr>
                    </thead>
                    <tbody id="ml-table"></tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- TIME SERIES -->
    <div class="section-title">📊 Analitik Historis</div>
    <div class="charts" style="margin-bottom:20px">
        <div class="card-base chart-box full-width">
            <h3>📈 Tren AQI Rata-rata per Jam (Historis)</h3>
            <canvas id="timeseriesChart" height="80"></canvas>
        </div>
    </div>

    <!-- MAP + TABLE (ML-synced) -->
    <div class="section-title">🗺️ Peta Distribusi & Detail Prediksi</div>
    <div class="info-row" style="margin-bottom:32px">
        <div class="card-base map-box">
            <h3>Peta Polusi Jakarta</h3>
            <div id="map"></div>
        </div>
        
    </div>

</div>

<script>
let timeseriesChart, mlPredChart, featureChart, map;
let markers = [];

Chart.defaults.color = '#94a3b8';
Chart.defaults.font.family = "'Inter', sans-serif";

function getAqiClass(aqi) {
    if (aqi <= 50)  return ['good',      'badge-good',      'Good'];
    if (aqi <= 100) return ['moderate',  'badge-moderate',  'Moderate'];
    if (aqi <= 150) return ['sensitive', 'badge-sensitive', 'Sensitive'];
    if (aqi <= 200) return ['unhealthy', 'badge-unhealthy', 'Unhealthy'];
    return ['very-bad', 'badge-very-bad', 'Very Unhealthy'];
}

function getAqiColor(aqi) {
    if (aqi <= 50)  return '#4ade80';
    if (aqi <= 100) return '#facc15';
    if (aqi <= 150) return '#fb923c';
    if (aqi <= 200) return '#f87171';
    return '#c084fc';
}

function getCongestionColor(pct) {
    if (pct >= 60) return '#f87171';
    if (pct >= 30) return '#fb923c';
    return '#4ade80';
}

function getUvLabel(uv) {
    if (uv <= 2)  return { label: 'Low',       color: '#4ade80' };
    if (uv <= 5)  return { label: 'Moderate',  color: '#facc15' };
    if (uv <= 7)  return { label: 'High',      color: '#fb923c' };
    if (uv <= 10) return { label: 'Very High', color: '#f87171' };
    return { label: 'Extreme', color: '#c084fc' };
}

function initMap() {
    map = L.map('map').setView([-6.2088, 106.8456], 11);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '© OpenStreetMap © CARTO'
    }).addTo(map);
}

function updateMap(mlPredictions) {
    markers.forEach(m => map.removeLayer(m));
    markers = [];

    if (!mlPredictions || mlPredictions.length === 0) return;

    mlPredictions.forEach(p => {
        if (!p.lat || !p.lon) return;
        const aqi   = Math.round(p.ml_predicted_aqi);
        const color = getAqiColor(aqi);
        const [, , label] = getAqiClass(aqi);

        const circle = L.circleMarker([p.lat, p.lon], {
            radius: 28,
            fillColor: color,
            color: '#ffffff',
            weight: 2,
            opacity: 0.8,
            fillOpacity: 0.4
        }).addTo(map);

        circle.bindPopup(`
            <div style="font-family:'Inter',sans-serif; color:#0f172a; min-width:160px">
                <b style="font-size:14px">${p.station}</b><br><br>
                AQI Aktual: <b>${Math.round(p.current_aqi)}</b><br>
                ML Prediksi: <b style="color:${color}">${aqi}</b><br>
                Kategori: <b>${label}</b><br>
                Delta: <b>${(p.ml_predicted_aqi - p.current_aqi).toFixed(1)}</b>
            </div>
        `);

        const icon = L.divIcon({
            className: '',
            html: `<div style="color:${color}; font-weight:700; font-size:13px; text-shadow:0 1px 4px rgba(0,0,0,0.8);">${aqi}</div>`,
            iconSize: [40, 20],
            iconAnchor: [20, 10]
        });
        L.marker([p.lat, p.lon], { icon }).addTo(map);
        markers.push(circle);
    });
}

async function loadData() {
    try {
        const res  = await fetch('/api/dashboard');
        const data = await res.json();

        document.getElementById('last-update').textContent =
            'Last update: ' + new Date().toLocaleTimeString('id-ID');

        // ── AQI CARDS ──
        document.getElementById('station-cards').innerHTML = data.summary.map(s => {
            const [cls, badge, label] = getAqiClass(s.avg_aqi);
            return `
            <div class="card-base card">
                <div class="station">${s.station.replace('jakarta-','')}</div>
                <div class="aqi-value ${cls}">${Math.round(s.avg_aqi)}</div>
                <span class="aqi-cat ${badge}">${label}</span>
                <div class="meta">
                    PM2.5: <b>${s.avg_pm25 ? s.avg_pm25.toFixed(1) : 'N/A'}</b> µg/m³<br>
                    Temp: <b>${s.avg_temp ? s.avg_temp.toFixed(1) : 'N/A'}</b> °C<br>
                    Records: ${s.total_records}
                </div>
            </div>`;
        }).join('');

        // ── WEATHER CARDS ──
        if (data.weather && data.weather.length > 0) {
            document.getElementById('weather-cards').innerHTML = data.weather.map(w => `
            <div class="card-base weather-card">
                <div class="w-station">${w.station ? w.station.replace('jakarta-','') : 'Jakarta'}</div>
                <div class="w-temp">${w.avg_temp ? w.avg_temp.toFixed(1) : 'N/A'}°C</div>
                <div class="w-desc">${w.weather_desc || 'N/A'}</div>
                <div class="w-meta">
                    💧 Humidity: <b>${w.avg_humidity ? w.avg_humidity.toFixed(1) : 'N/A'}%</b><br>
                    🌬️ Wind: <b>${w.avg_wind_speed ? w.avg_wind_speed.toFixed(1) : 'N/A'} m/s</b><br>
                    🌡️ Feels: <b>${w.avg_feels_like ? w.avg_feels_like.toFixed(1) : 'N/A'}°C</b><br>
                    ☁️ Clouds: <b>${w.avg_clouds ? w.avg_clouds.toFixed(0) : 'N/A'}%</b>
                </div>
            </div>`).join('');
        } else {
            document.getElementById('weather-cards').innerHTML =
                '<div style="color:var(--text-muted);">Data cuaca belum tersedia</div>';
        }

        // ── TRAFFIC ──
        if (data.traffic && data.traffic.length > 0) {
            document.getElementById('traffic-list').innerHTML = data.traffic.map(t => {
                const color = getCongestionColor(t.avg_congestion);
                const pct   = Math.round(t.avg_congestion);
                return `
                <div class="traffic-item">
                    <div>
                        <div class="traffic-point">${t.point}</div>
                        <div class="traffic-speed">${t.avg_speed ? t.avg_speed.toFixed(1) : 'N/A'} km/h · ${t.traffic_status}</div>
                    </div>
                    <div class="traffic-right">
                        <div style="color:${color}; font-weight:700; font-size:1.1rem">${pct}%</div>
                        <div class="congestion-bar-bg">
                            <div class="congestion-bar" style="width:${Math.min(pct,100)}%; background:${color}; box-shadow:0 0 8px ${color}80"></div>
                        </div>
                    </div>
                </div>`;
            }).join('');
        }

        // ── UV ──
        if (data.uv) {
            const { label, color } = getUvLabel(data.uv.avg_uv);
            document.getElementById('uv-info').innerHTML = `
                <div class="uv-big" style="color:${color}; text-shadow:0 0 20px ${color}40">${data.uv.avg_uv ? data.uv.avg_uv.toFixed(1) : '0.0'}</div>
                <div class="uv-label" style="color:${color}">${label}</div>
                <div class="uv-meta">
                    📈 UV Max Hari Ini: <b style="color:var(--text-main)">${data.uv.max_uv ? data.uv.max_uv.toFixed(2) : 'N/A'}</b><br>
                    🌿 Ozon: <b style="color:var(--text-main)">${data.uv.avg_ozone ? data.uv.avg_ozone.toFixed(1) : 'N/A'} DU</b><br>
                    ☀️ Safe Exposure: <b style="color:var(--text-main)">${data.uv.avg_uv == 0 ? 'Aman (Malam hari)' : data.uv.safe_exposure + ' menit'}</b>
                </div>`;
        }

    } catch (e) { console.error("Error loading dashboard:", e); }
}

async function loadTimeSeries() {
    try {
        const res  = await fetch('/api/timeseries');
        const data = await res.json();

        const labels = data.map(d => {
            const dt = new Date(d.hour_bucket.replace(' ', 'T') + ':00Z');
            const wib = new Date(dt.getTime() + 7 * 60 * 60 * 1000);
            return wib.toLocaleString('id-ID', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
        });
        const avgAqi = data.map(d => Math.round(d.avg_aqi));
        const maxAqi = data.map(d => d.max_aqi);
        const minAqi = data.map(d => d.min_aqi);
        const colors = avgAqi.map(getAqiColor);

        if (timeseriesChart) timeseriesChart.destroy();
        timeseriesChart = new Chart(document.getElementById('timeseriesChart'), {
            type: 'line',
            data: {
                labels,
                datasets: [
                    {
                        label: 'AQI Max',
                        data: maxAqi,
                        borderColor: '#f87171',
                        backgroundColor: 'rgba(248,113,113,0.05)',
                        borderWidth: 1.5,
                        borderDash: [5, 5],
                        pointRadius: 2,
                        tension: 0.4,
                        fill: false,
                    },
                    {
                        label: 'AQI Rata-rata',
                        data: avgAqi,
                        borderColor: '#38bdf8',
                        backgroundColor: 'rgba(56,189,248,0.15)',
                        borderWidth: 3,
                        pointRadius: 5,
                        pointBackgroundColor: colors,
                        pointBorderColor: '#fff',
                        pointBorderWidth: 1.5,
                        tension: 0.4,
                        fill: true,
                    },
                    {
                        label: 'AQI Min',
                        data: minAqi,
                        borderColor: '#4ade80',
                        backgroundColor: 'rgba(74,222,128,0.05)',
                        borderWidth: 1.5,
                        borderDash: [5, 5],
                        pointRadius: 2,
                        tension: 0.4,
                        fill: false,
                    },
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top', labels: { usePointStyle: true, boxWidth: 8 } },
                    tooltip: {
                        backgroundColor: 'rgba(15,23,42,0.9)',
                        padding: 12,
                        cornerRadius: 8,
                        callbacks: {
                            afterLabel: function(ctx) {
                                if (ctx.datasetIndex !== 1) return null;
                                const aqi = ctx.parsed.y;
                                if (aqi <= 50)  return 'Status: Good';
                                if (aqi <= 100) return 'Status: Moderate';
                                if (aqi <= 150) return 'Status: Sensitive';
                                if (aqi <= 200) return 'Status: Unhealthy';
                                return 'Status: Very Unhealthy';
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: false,
                        grid: { color: 'rgba(255,255,255,0.05)' },
                        title: { display: true, text: 'Nilai AQI', color: '#64748b', font: { weight: '600' } }
                    },
                    x: { grid: { display: false }, ticks: { maxRotation: 45, minRotation: 45 } }
                }
            }
        });
    } catch (e) { console.error("Error loading time series:", e); }
}

async function loadML() {
    try {
        const res  = await fetch('/api/ml');
        const data = await res.json();
        if (!data.predictions || data.predictions.length === 0) return;

        const preds   = data.predictions;
        const metrics = data.metadata.model_metrics || {};
        const fimp    = data.metadata.feature_importance || {};

        // ── ML PRED CHART ──
        const labels  = preds.map(p => p.station.replace('jakarta-','').toUpperCase());
        const current = preds.map(p => Math.round(p.current_aqi));
        const mlPred  = preds.map(p => Math.round(p.ml_predicted_aqi));

        if (mlPredChart) mlPredChart.destroy();
        mlPredChart = new Chart(document.getElementById('mlPredChart'), {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    { label: 'AQI Aktual',    data: current, backgroundColor: 'rgba(56,189,248,0.8)',  borderRadius: 8 },
                    { label: 'ML Prediction', data: mlPred,  backgroundColor: 'rgba(245,158,11,0.8)', borderRadius: 8 }
                ]
            },
            options: {
                responsive: true,
                plugins: { legend: { labels: { usePointStyle: true, boxWidth: 8 } } },
                scales: {
                    y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' } },
                    x: { grid: { display: false } }
                }
            }
        });

        // ── FEATURE IMPORTANCE ──
        const fimpEntries = Object.entries(fimp).sort((a,b) => b[1]-a[1]);
        const fimpLabels  = fimpEntries.map(([k]) => k);
        const fimpValues  = fimpEntries.map(([,v]) => (v*100).toFixed(2));
        const fimpColors  = ['#f59e0b','#38bdf8','#818cf8','#22c55e','#f97316',
                             '#ef4444','#a855f7','#06b6d4','#84cc16','#ec4899'];

        if (featureChart) featureChart.destroy();
        featureChart = new Chart(document.getElementById('featureChart'), {
            type: 'bar',
            data: {
                labels: fimpLabels,
                datasets: [{
                    label: 'Importance (%)',
                    data: fimpValues,
                    backgroundColor: fimpColors,
                    borderRadius: 6,
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    x: { grid: { color: 'rgba(255,255,255,0.05)' } },
                    y: { grid: { display: false } }
                }
            }
        });

        // ── MODEL METRICS ──
        document.getElementById('model-metrics').innerHTML = `
            <div class="traffic-item">
                <div class="traffic-point">R² Score</div>
                <div style="color:#4ade80; font-weight:700; font-size:1.4rem">${metrics.r2 || 'N/A'}</div>
            </div>
            <div class="traffic-item">
                <div class="traffic-point">MAE (Mean Absolute Error)</div>
                <div style="color:#38bdf8; font-weight:600">${metrics.mae || 'N/A'}</div>
            </div>
            <div class="traffic-item">
                <div class="traffic-point">RMSE</div>
                <div style="color:#818cf8; font-weight:600">${metrics.rmse || 'N/A'}</div>
            </div>
            <div class="traffic-item">
                <div class="traffic-point">Training Samples</div>
                <div style="color:var(--text-muted)">${metrics.training_samples ? metrics.training_samples.toLocaleString() : 'N/A'}</div>
            </div>
            <div class="traffic-item">
                <div class="traffic-point">Test Samples</div>
                <div style="color:var(--text-muted)">${metrics.test_samples ? metrics.test_samples.toLocaleString() : 'N/A'}</div>
            </div>
            <div class="traffic-item">
                <div class="traffic-point">Algorithm</div>
                <div style="color:#f59e0b; font-weight:600">Random Forest Regressor</div>
            </div>
        `;

        // ── ML TABLE ──
        document.getElementById('ml-table').innerHTML = preds.map(p => {
            const delta    = (p.ml_predicted_aqi - p.current_aqi).toFixed(1);
            const deltaCol = delta > 0 ? '#f87171' : '#4ade80';
            const deltaStr = delta > 0 ? `▲ +${delta}` : `▼ ${delta}`;
            const [cls, badge, label] = getAqiClass(p.ml_predicted_aqi);
            return `
            <tr>
                <td>${p.station}</td>
                <td class="${getAqiClass(p.current_aqi)[0]}">${Math.round(p.current_aqi)}</td>
                <td class="${cls}" style="font-weight:700">${Math.round(p.ml_predicted_aqi)}</td>
                <td><span class="aqi-cat ${badge}">${label}</span></td>
                <td style="color:${deltaCol}; font-weight:600">${deltaStr}</td>
            </tr>`;
        }).join('');

        // ── UPDATE PETA ──
        updateMap(preds);

        // ── MAP PRED TABLE ──
        document.getElementById('map-pred-table').innerHTML = preds.map(p => {
            const delta    = (p.ml_predicted_aqi - p.current_aqi).toFixed(1);
            const deltaCol = delta > 0 ? '#f87171' : '#4ade80';
            const deltaStr = delta > 0 ? `▲ +${delta}` : `▼ ${delta}`;
            const [cls, badge, label] = getAqiClass(p.ml_predicted_aqi);
            return `
            <tr>
                <td>${p.station}</td>
                <td class="${getAqiClass(p.current_aqi)[0]}">${Math.round(p.current_aqi)}</td>
                <td class="${cls}" style="font-weight:700">${Math.round(p.ml_predicted_aqi)}</td>
                <td><span class="aqi-cat ${badge}">${label}</span></td>
                <td style="color:${deltaCol}; font-weight:600">${deltaStr}</td>
            </tr>`;
        }).join('');

    } catch(e) { console.error('ML load error:', e); }
}

function refreshAll() {
    loadData();
    loadTimeSeries();
    loadML();
}

// Init
initMap();
loadData();
loadTimeSeries();
loadML();
setInterval(loadData, 60000);
setInterval(loadTimeSeries, 300000);
setInterval(loadML, 300000);
</script>
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/ml')
def api_ml():
    try:
        result = subprocess.run(
            ["docker", "exec", "aira-namenode", "hdfs", "dfs",
             "-ls", "/aira/processed/ml_predictions"],
            capture_output=True, text=True, check=True
        )
        files = [l.split()[-1] for l in result.stdout.strip().split('\n') if '.json' in l]
        latest = sorted(files)[-1] if files else None

        predictions = []
        if latest:
            cat = subprocess.run(
                ["docker", "exec", "aira-namenode", "hdfs", "dfs", "-cat", latest],
                capture_output=True, text=True, check=True
            )
            for line in cat.stdout.strip().split('\n'):
                if line:
                    predictions.append(json.loads(line))

        meta_result = subprocess.run(
            ["docker", "exec", "aira-namenode", "hdfs", "dfs",
             "-cat", "/aira/processed/ml_metadata/metadata.json"],
            capture_output=True, text=True, check=True
        )
        metadata = json.loads(meta_result.stdout)

    except Exception as e:
        print(f"[ERROR] ML API: {e}")
        predictions = []
        metadata = {}

    return jsonify({
        "predictions": predictions,
        "metadata"   : metadata,
    })

@app.route('/api/dashboard')
def api_dashboard():
    summary  = read_hdfs_json("hdfs://namenode:9000/aira/processed/station_summary")
    traffic  = read_hdfs_json("hdfs://namenode:9000/aira/processed/traffic_summary")
    uv_data  = read_hdfs_json("hdfs://namenode:9000/aira/processed/uv_summary")
    weather  = read_hdfs_json("hdfs://namenode:9000/aira/processed/weather_summary")

    return jsonify({
        "summary"  : summary,
        "traffic"  : traffic,
        "uv"       : uv_data[0] if uv_data else None,
        "weather"  : weather,
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route('/api/timeseries')
def api_timeseries():
    records = read_hdfs_json("hdfs://namenode:9000/aira/processed/timeseries")
    records.sort(key=lambda x: x.get('hour_bucket', ''))
    return jsonify(records)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)