import json
import time
import requests
import os
from kafka import KafkaProducer
from datetime import datetime

# ─────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────
KAFKA_BROKER        = os.environ.get('KAFKA_BROKER', 'localhost:9092')
INTERVAL_SEC        = 30

AQICN_TOKEN         = os.environ.get('AQICN_TOKEN', 'your_aqicn_token')
OPENWEATHER_KEY     = os.environ.get('OPENWEATHER_KEY', 'your_openweather_key')
TOMTOM_KEY          = os.environ.get('TOMTOM_KEY', 'your_tomtom_key')
OPENUV_KEY          = os.environ.get('OPENUV_KEY', 'your_openuv_key')

# Stasiun pemantau Jakarta
STATIONS = [
    ("jakarta-south",   -6.2615, 106.8106),
    ("jakarta-central", -6.1862, 106.8063),
    ("jakarta-north",   -6.1344, 106.8446),
    ("jakarta-east",    -6.2250, 106.9004),
    ("jakarta-west",    -6.1675, 106.7513),
]

# Koridor lalu lintas utama Jakarta
TRAFFIC_POINTS = [
    ("sudirman",        -6.2088, 106.8228),
    ("thamrin",         -6.1944, 106.8229),
    ("gatot-subroto",   -6.2297, 106.8253),
    ("tb-simatupang",   -6.3044, 106.7935),
    ("bekasi-barat",    -6.2383, 106.9756),
]

# Koordinat pusat Jakarta untuk UV index
JAKARTA_CENTER = (-6.2088, 106.8456)

# ─────────────────────────────────────────
# INISIALISASI KAFKA PRODUCER
# ─────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ─────────────────────────────────────────
# FETCH FUNCTIONS
# ─────────────────────────────────────────
def fetch_aqi(station_name, lat, lon):
    url = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={AQICN_TOKEN}"
    try:
        data = requests.get(url, timeout=10).json()
        if data['status'] != 'ok':
            return None
        d = data['data']
        iaqi = d.get('iaqi', {})
        return {
            "station"   : station_name,
            "timestamp" : datetime.utcnow().isoformat(),
            "aqi"       : d.get('aqi'),
            "pm25"      : iaqi.get('pm25', {}).get('v'),
            "pm10"      : iaqi.get('pm10', {}).get('v'),
            "no2"       : iaqi.get('no2',  {}).get('v'),
            "so2"       : iaqi.get('so2',  {}).get('v'),
            "co"        : iaqi.get('co',   {}).get('v'),
            "o3"        : iaqi.get('o3',   {}).get('v'),
            "temp"      : iaqi.get('t',    {}).get('v'),
            "humidity"  : iaqi.get('h',    {}).get('v'),
            "wind"      : iaqi.get('w',    {}).get('v'),
            "lat"       : lat,
            "lon"       : lon,
        }
    except Exception as e:
        print(f"[ERROR] AQI {station_name}: {e}")
        return None

def fetch_weather(station_name, lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_KEY}&units=metric"
    try:
        data = requests.get(url, timeout=10).json()
        return {
            "station"       : station_name,
            "timestamp"     : datetime.utcnow().isoformat(),
            "lat"           : lat,
            "lon"           : lon,
            "temp"          : data['main']['temp'],
            "feels_like"    : data['main']['feels_like'],
            "humidity"      : data['main']['humidity'],
            "pressure"      : data['main']['pressure'],
            "wind_speed"    : data['wind']['speed'],
            "wind_deg"      : data['wind'].get('deg', 0),
            "wind_gust"     : data['wind'].get('gust', 0),
            "clouds"        : data['clouds']['all'],
            "visibility"    : data.get('visibility', 0),
            "rain_1h"       : data.get('rain', {}).get('1h', 0),
            "weather_main"  : data['weather'][0]['main'],
            "weather_desc"  : data['weather'][0]['description'],
        }
    except Exception as e:
        print(f"[ERROR] Weather {station_name}: {e}")
        return None

def fetch_traffic(point_name, lat, lon):
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?point={lat},{lon}&key={TOMTOM_KEY}"
    try:
        data = requests.get(url, timeout=10).json()
        flow = data.get('flowSegmentData', {})
        current_speed   = flow.get('currentSpeed', 0)
        free_flow_speed = flow.get('freeFlowSpeed', 1)
        congestion      = round((1 - current_speed / free_flow_speed) * 100, 1) if free_flow_speed > 0 else 0
        return {
            "point"             : point_name,
            "timestamp"         : datetime.utcnow().isoformat(),
            "lat"               : lat,
            "lon"               : lon,
            "current_speed"     : current_speed,
            "free_flow_speed"   : free_flow_speed,
            "current_travel_time": flow.get('currentTravelTime', 0),
            "free_flow_travel_time": flow.get('freeFlowTravelTime', 0),
            "congestion_pct"    : max(0, congestion),
            "confidence"        : flow.get('confidence', 0),
        }
    except Exception as e:
        print(f"[ERROR] Traffic {point_name}: {e}")
        return None

def fetch_uv(lat, lon):
    url = f"https://api.openuv.io/api/v1/uv?lat={lat}&lng={lon}"
    headers = {"x-access-token": OPENUV_KEY}
    try:
        data = requests.get(url, timeout=10, headers=headers).json()
        result = data.get('result', {})
        return {
            "timestamp"     : datetime.utcnow().isoformat(),
            "lat"           : lat,
            "lon"           : lon,
            "uv"            : result.get('uv', 0),
            "uv_max"        : result.get('uv_max', 0),
            "uv_time"       : result.get('uv_time', ''),
            "ozone"         : result.get('ozone', 0),
            "safe_exposure" : result.get('safe_exposure_time', {}).get('st3', 0),
        }
    except Exception as e:
        print(f"[ERROR] UV: {e}")
        return None

# ─────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────
def main():
    print(f"[INFO] Producer started — broker: {KAFKA_BROKER}")
    print(f"[INFO] Interval: {INTERVAL_SEC}s")
    print("-" * 50)

    while True:
        timestamp = datetime.utcnow().isoformat()
        print(f"\n[CYCLE] {timestamp}")

        # 1. AQI
        for name, lat, lon in STATIONS:
            record = fetch_aqi(name, lat, lon)
            if record:
                producer.send('aira-aqi-raw', value=record)
                print(f"  [AQI]     {name} | AQI: {record['aqi']} | PM2.5: {record['pm25']}")

        # 2. Weather
        for name, lat, lon in STATIONS:
            record = fetch_weather(name, lat, lon)
            if record:
                producer.send('aira-weather-raw', value=record)
                print(f"  [WEATHER] {name} | {record['temp']}°C | Wind: {record['wind_speed']}m/s | {record['weather_desc']}")

        # 3. Traffic
        for name, lat, lon in TRAFFIC_POINTS:
            record = fetch_traffic(name, lat, lon)
            if record:
                producer.send('aira-traffic-raw', value=record)
                print(f"  [TRAFFIC] {name} | Speed: {record['current_speed']}km/h | Congestion: {record['congestion_pct']}%")

        # 4. UV (hanya dari pusat Jakarta)
        record = fetch_uv(*JAKARTA_CENTER)
        if record:
            producer.send('aira-uv-raw', value=record)
            print(f"  [UV]      Index: {record['uv']} | Max: {record['uv_max']} | Ozone: {record['ozone']}DU")

        producer.flush()
        print(f"[INFO] Waiting {INTERVAL_SEC}s...")
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()
