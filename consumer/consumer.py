import json
import subprocess
import os
from kafka import KafkaConsumer
from datetime import datetime

# ─────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────
KAFKA_BROKER  = os.environ.get('KAFKA_BROKER', 'localhost:9092')
KAFKA_GROUP   = 'aira-consumer-group'
BATCH_SIZE    = 10

TOPICS = {
    'aira-aqi-raw'     : '/aira/raw/aqi',
    'aira-weather-raw' : '/aira/raw/weather',
    'aira-traffic-raw' : '/aira/raw/traffic',
    'aira-uv-raw'      : '/aira/raw/uv',
}

# ─────────────────────────────────────────
# INISIALISASI KAFKA CONSUMER
# ─────────────────────────────────────────
consumer = KafkaConsumer(
    *TOPICS.keys(),
    bootstrap_servers=KAFKA_BROKER,
    group_id=KAFKA_GROUP,
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def save_to_hdfs(records, hdfs_base):
    """Simpan batch records ke HDFS via docker exec"""
    try:
        now = datetime.utcnow()
        path = f"{hdfs_base}/year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/hour={now.strftime('%H')}"
        filename = f"{path}/data_{now.strftime('%Y%m%d_%H%M%S')}.jsonl"
        content = '\n'.join(json.dumps(r) for r in records) + '\n'

        subprocess.run(
            ["docker", "exec", "aira-namenode", "hdfs", "dfs", "-mkdir", "-p", path],
            check=True, capture_output=True
        )
        subprocess.run(
            ["docker", "exec", "-i", "aira-namenode", "hdfs", "dfs", "-put", "-f", "-", filename],
            input=content.encode('utf-8'),
            check=True, capture_output=True
        )
        print(f"[HDFS] ✓ {len(records)} records → {filename}")
        return filename

    except subprocess.CalledProcessError as e:
        print(f"[HDFS ERROR] {e.stderr.decode()}")
        return None
    except Exception as e:
        print(f"[HDFS ERROR] {type(e).__name__}: {e}")
        return None

def main():
    print(f"[INFO] Consumer started — topics: {list(TOPICS.keys())}")
    print(f"[INFO] Batch size: {BATCH_SIZE}")
    print("-" * 50)

    # Buffer per topic
    buffers = {topic: [] for topic in TOPICS.keys()}

    for message in consumer:
        topic  = message.topic
        record = message.value

        buffers[topic].append(record)

        # Label untuk logging
        if topic == 'aira-aqi-raw':
            label = f"AQI: {record.get('aqi')} | {record.get('station')}"
        elif topic == 'aira-weather-raw':
            label = f"Temp: {record.get('temp')}°C | {record.get('station')}"
        elif topic == 'aira-traffic-raw':
            label = f"Congestion: {record.get('congestion_pct')}% | {record.get('point')}"
        elif topic == 'aira-uv-raw':
            label = f"UV: {record.get('uv')} | Max: {record.get('uv_max')}"
        else:
            label = str(record)

        print(f"[{topic.replace('aira-','').replace('-raw','').upper():8}] {label}")

        # Flush ke HDFS kalau buffer penuh
        if len(buffers[topic]) >= BATCH_SIZE:
            save_to_hdfs(buffers[topic], TOPICS[topic])
            buffers[topic].clear()

if __name__ == "__main__":
    main()
