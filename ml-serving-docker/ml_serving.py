import json
import subprocess
import joblib
import numpy as np
import pandas as pd
from datetime import datetime
import os

# ─────────────────────────────────────────
# LOAD MODEL & ENCODER
# ─────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

model      = joblib.load(os.path.join(BASE_DIR, 'aira_rf_model.pkl'))
le_stasiun = joblib.load(os.path.join(BASE_DIR, 'aira_le_stasiun.pkl'))

with open(os.path.join(BASE_DIR, 'feature_importance.json')) as f:
    feature_importance = json.load(f)

with open(os.path.join(BASE_DIR, 'model_metrics.json')) as f:
    model_metrics = json.load(f)

print("[INFO] Model loaded successfully")
print(f"[INFO] R² = {model_metrics['r2']} | MAE = {model_metrics['mae']} | RMSE = {model_metrics['rmse']}")

# ─────────────────────────────────────────
# MAPPING STASIUN AIRA → STASIUN HISTORIS
# ─────────────────────────────────────────
STATION_MAPPING = {
    'jakarta-north'  : 'Kelapa Gading',
    'jakarta-central': 'Bundaran HI',
    'jakarta-south'  : 'Jagakarsa',
    'jakarta-east'   : 'Lubang Buaya',
    'jakarta-west'   : 'Kebon Jeruk',
}

# Koordinat tetap per stasiun
STATION_COORDS = {
    'jakarta-north'  : (-6.1344, 106.8446),
    'jakarta-central': (-6.1862, 106.8063),
    'jakarta-south'  : (-6.2615, 106.8106),
    'jakarta-east'   : (-6.2250, 106.9004),
    'jakarta-west'   : (-6.1675, 106.7513),
}

def read_hdfs_latest(hdfs_path):
    """Baca data terbaru dari HDFS"""
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

        if not files:
            return []

        latest_file = sorted(files)[-1]
        cat_result = subprocess.run(
            ["docker", "exec", "aira-namenode", "hdfs", "dfs", "-cat", latest_file],
            capture_output=True, text=True, check=True
        )
        records = []
        for line in cat_result.stdout.strip().split('\n'):
            if line:
                records.append(json.loads(line))
        return records
    except Exception as e:
        print(f"[ERROR] {e}")
        return []

def get_latest_aqi():
    """Baca data AQI terbaru dari HDFS processed"""
    try:
        result = subprocess.run(
            ["docker", "exec", "aira-namenode", "hdfs", "dfs",
             "-ls", "/aira/processed/station_summary"],
            capture_output=True, text=True, check=True
        )
        files = [l.split()[-1] for l in result.stdout.strip().split('\n') if '.json' in l]
        records = []
        for f in files:
            cat = subprocess.run(
                ["docker", "exec", "aira-namenode", "hdfs", "dfs", "-cat", f],
                capture_output=True, text=True, check=True
            )
            for line in cat.stdout.strip().split('\n'):
                if line:
                    records.append(json.loads(line))
        return records
    except Exception as e:
        print(f"[ERROR] {e}")
        return []

def predict_aqi(station_aira, pm10, so2, co, o3, no2):
    """Generate prediksi menggunakan RF model"""
    now = datetime.now()

    station_hist = STATION_MAPPING.get(station_aira, 'Bundaran HI')

    try:
        stasiun_enc = le_stasiun.transform([station_hist])[0]
    except:
        stasiun_enc = 0

    is_dry_season = 1 if now.month in [6, 7, 8, 9, 10] else 0

    features = pd.DataFrame([{
        'pm10'         : pm10 or 0,
        'so2'          : so2  or 0,
        'co'           : co   or 0,
        'o3'           : o3   or 0,
        'no2'          : no2  or 0,
        'month'        : now.month,
        'dayofweek'    : now.weekday(),
        'year'         : now.year,
        'is_dry_season': is_dry_season,
        'stasiun_enc'  : stasiun_enc,
    }])

    predicted_max = float(model.predict(features)[0])
    return round(predicted_max, 2)

def get_aqi_category(aqi):
    if aqi <= 50:  return "Good"
    if aqi <= 100: return "Moderate"
    if aqi <= 150: return "Unhealthy for Sensitive Groups"
    if aqi <= 200: return "Unhealthy"
    if aqi <= 300: return "Very Unhealthy"
    return "Hazardous"

def save_to_hdfs(content, hdfs_path):
    """Simpan JSON ke HDFS"""
    try:
        subprocess.run(
            ["docker", "exec", "aira-namenode", "hdfs", "dfs",
             "-mkdir", "-p", os.path.dirname(hdfs_path)],
            check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "exec", "-i", "aira-namenode", "hdfs", "dfs",
             "-put", "-f", "-", hdfs_path],
            input=content.encode('utf-8'),
            check=True, capture_output=True
        )
        print(f"[HDFS] ✓ Saved → {hdfs_path}")
    except Exception as e:
        print(f"[HDFS ERROR] {e}")

def main():
    print("\n" + "=" * 50)
    print("AIRA ML Serving Started")
    print("=" * 50)

    aqi_records = get_latest_aqi()
    if not aqi_records:
        print("[WARN] No AQI data found in HDFS")
        return

    predictions = []
    print("\n[PREDICT] Generating ML predictions...")

    for record in aqi_records:
        station = record.get('station')
        predicted = predict_aqi(
            station_aira = station,
            pm10 = record.get('avg_pm25'),
            so2  = 0,
            co   = 0,
            o3   = record.get('avg_aqi', 0) * 0.01,
            no2  = 0,
        )

        category = get_aqi_category(predicted)

        # Ambil koordinat dari STATION_COORDS, bukan dari record
        lat, lon = STATION_COORDS.get(station, (None, None))

        predictions.append({
            "station"          : station,
            "current_aqi"      : round(record.get('avg_aqi', 0), 2),
            "ml_predicted_aqi" : predicted,
            "ml_category"      : category,
            "lat"              : lat,
            "lon"              : lon,
            "timestamp"        : datetime.utcnow().isoformat(),
        })
        print(f"  {station:20} | Current: {record.get('avg_aqi', 0):.1f} | ML Pred: {predicted} | {category} | lat={lat}, lon={lon}")

    now = datetime.utcnow()
    hdfs_path = f"/aira/processed/ml_predictions/predictions_{now.strftime('%Y%m%d_%H%M%S')}.json"
    content = '\n'.join(json.dumps(p) for p in predictions)
    save_to_hdfs(content, hdfs_path)

    metadata = {
        "timestamp"          : now.isoformat(),
        "model_metrics"      : model_metrics,
        "feature_importance" : feature_importance,
        "total_predictions"  : len(predictions),
    }
    save_to_hdfs(
        json.dumps(metadata, indent=2),
        "/aira/processed/ml_metadata/metadata.json"
    )

    print("\n[INFO] ML Serving completed!")
    print(f"[INFO] {len(predictions)} predictions saved to HDFS")

if __name__ == "__main__":
    main()
