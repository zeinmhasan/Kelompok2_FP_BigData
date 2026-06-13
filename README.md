# AIRA — AI-Integrated Real-Time Air Quality Forecasting System
### Jakarta Air Quality Intelligence System

![Big Data](https://img.shields.io/badge/Big%20Data-Apache%20Kafka%20%7C%20HDFS%20%7C%20Spark-orange)
![ML](https://img.shields.io/badge/ML-Random%20Forest%20R²%3D0.98-green)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Python](https://img.shields.io/badge/Python-3.11-yellow)

> **Final Project Big Data — Kelas B**  
> Sistem monitoring dan prediksi kualitas udara Jakarta secara real-time berbasis arsitektur Big Data dengan integrasi Machine Learning.

---
## 👥 Anggota Kelompok

| Nama | NRP |
|---|---|
| Zein Muhammad Hasan | 5027241035 |
| Andi Naufal Zaky | 5027241059 |
| Naila Cahyarani Idelia | 5027241063 |
| Aslam Ahmad Usman | 5027241074 |
| Muhammad Ahsani Taqwiim Rakhman | 5027241099 |


---
## 📋 Daftar Isi

- [Tentang Proyek](#-tentang-proyek)
- [Arsitektur Sistem](#-arsitektur-sistem)
- [Fitur Dashboard](#-fitur-dashboard)
- [Tech Stack](#-tech-stack)
- [Struktur Folder](#-struktur-folder)
- [Prasyarat](#-prasyarat)
- [Cara Menjalankan](#-cara-menjalankan)
- [Konfigurasi API Key](#-konfigurasi-api-key)
- [Akses Web UI](#-akses-web-ui)
- [Pipeline Data](#-pipeline-data)
- [Model Machine Learning](#-model-machine-learning)
- [Anggota Kelompok](#-anggota-kelompok)

---

## 🌫️ Tentang Proyek

Jakarta secara konsisten masuk dalam daftar kota dengan kualitas udara terburuk di dunia. AIRA hadir sebagai solusi monitoring terpadu yang:

- **Memantau** kualitas udara Jakarta secara real-time dari 5 stasiun
- **Mengintegrasikan** data dari 4 sumber: AQI, cuaca, lalu lintas, dan indeks UV
- **Memprediksi** AQI menggunakan model Random Forest yang dilatih pada 15 tahun data historis Jakarta (2010–2025)
- **Memvisualisasikan** semua data dalam dashboard interaktif dengan peta, chart, dan tabel

---

## 🏗️ Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                          │
│  AQICN API │ OpenWeatherMap │ TomTom Traffic │ OpenUV API   │
└──────────────────────┬──────────────────────────────────────┘
                       │ (tiap 30 detik)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA (Message Broker)                     │
│  Topic: aira-aqi-raw │ aira-weather-raw                     │
│          aira-traffic-raw │ aira-uv-raw                     │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  HDFS (Distributed Storage)                   │
│  /aira/raw/aqi │ /aira/raw/weather                          │
│  /aira/raw/traffic │ /aira/raw/uv                           │
└──────────────────────┬──────────────────────────────────────┘
                       │ (tiap 5 menit)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   APACHE SPARK (Processing)                   │
│  - Agregasi per stasiun                                      │
│  - Time series per jam                                       │
│  - Traffic & UV summary                                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          ▼                         ▼
┌─────────────────┐       ┌──────────────────────┐
│   ML SERVING    │       │   HDFS (Processed)    │
│ Random Forest   │       │  /aira/processed/     │
│  R² = 0.98      │       │  station_summary      │
│                 │       │  predictions          │
│                 │       │  traffic_summary      │
│                 │       │  uv_summary           │
│                 │       │  weather_summary      │
│                 │       │  timeseries           │
│                 │       │  ml_predictions       │
└────────┬────────┘       └──────────┬───────────┘
         └────────────┬──────────────┘
                      ▼
┌─────────────────────────────────────────────────────────────┐
│              FLASK DASHBOARD (localhost:5000)                 │
│  AQI Cards │ Weather │ Traffic │ UV │ Map │ ML Predictions  │
└─────────────────────────────────────────────────────────────┘
```

---

## 📊 Fitur Dashboard

| Fitur | Deskripsi |
|---|---|
| 🌫️ AQI Cards | Nilai AQI real-time per stasiun dengan kategori warna |
| 🌤️ Weather Cards | Suhu, kelembaban, angin, dan kondisi cuaca per stasiun |
| 🚗 Traffic Bars | Persentase kemacetan 5 koridor utama Jakarta |
| ☀️ UV Index | Indeks UV dan kadar ozon saat ini |
| 🤖 ML Predictions | Prediksi AQI dari Random Forest + feature importance |
| 📈 Time Series | Tren AQI historis per jam (min, rata-rata, max) |
| 🗺️ Peta Interaktif | Heatmap polusi Jakarta dengan Leaflet.js |

---

## 🛠️ Tech Stack

| Komponen | Teknologi |
|---|---|
| Message Broker | Apache Kafka 7.5.0 + Zookeeper |
| Distributed Storage | HDFS (Hadoop 3.2.1) |
| Batch Processing | Apache Spark 3.3.0 |
| ML Framework | Scikit-learn (Random Forest) |
| Backend | Python 3.11 + Flask |
| Frontend | Chart.js + Leaflet.js |
| Containerization | Docker + Docker Compose |
| Data Sources | AQICN, OpenWeatherMap, TomTom, OpenUV |

---

## 📁 Struktur Folder

```
aira-project/
├── docker-compose.yml          # Orchestrasi semua service
├── .env                        # API keys & konfigurasi (tidak di-push)
├── .env.example                # Template konfigurasi
├── producer/                   # Fetch data dari API → Kafka
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py
├── consumer/                   # Kafka → HDFS
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py
├── spark/                      # Spark batch processing job
│   ├── spark_job.py
│   ├── feature_importance.json
│   └── model_metrics.json
├── scheduler/                  # Trigger Spark + ML tiap 5 menit
│   ├── Dockerfile
│   ├── requirements.txt
│   └── scheduler.py
├── dashboard/                  # Flask web dashboard
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
└── ml-serving-docker/          # ML inference container
    ├── Dockerfile
    ├── requirements.txt
    ├── ml_serving.py
    ├── feature_importance.json
    └── model_metrics.json
```

---

## ✅ Prasyarat

Pastikan sudah terinstall di sistem kamu:

- **Docker** >= 24.x
- **Docker Compose** >= 2.x
- **WSL Ubuntu** (untuk Windows) atau Linux/macOS
- **Git**

Cek versi:
```bash
docker --version
docker compose version
```

---

## 🚀 Cara Menjalankan

### 1. Clone Repository

```bash
git clone https://github.com/zeinmhasan/Kelompok2_FP_BigData.git
cd Kelompok2_FP_BigData
```

### 2. Konfigurasi API Key

Salin file template dan isi dengan API key kamu:

```bash
cp .env.example .env
nano .env
```

Isi keempat API key (lihat bagian [Konfigurasi API Key](#-konfigurasi-api-key) untuk cara mendapatkannya).

### 3. Tambahkan Model ML

Karena file model (`.pkl`) tidak di-push ke GitHub karena ukurannya besar, kamu perlu menyediakan sendiri atau melatih ulang:

```
ml-serving-docker/
├── aira_rf_model.pkl       ← taruh di sini
└── aira_le_stasiun.pkl     ← taruh di sini
```

> Model dilatih menggunakan dataset historis AQI Jakarta 2010–2025 dengan fitur: `pm10`, `so2`, `co`, `o3`, `no2`, `month`, `dayofweek`, `year`, `is_dry_season`, `stasiun_enc`

### 4. Jalankan Semua Service

```bash
docker compose up -d
```

Perintah ini akan menjalankan **10 container** sekaligus:

| Container | Fungsi |
|---|---|
| aira-zookeeper | Koordinasi Kafka |
| aira-kafka | Message broker |
| aira-namenode | HDFS master node |
| aira-datanode | HDFS data node |
| aira-spark-master | Spark cluster master |
| aira-spark-worker | Spark executor |
| aira-producer | Fetch API → Kafka |
| aira-consumer | Kafka → HDFS |
| aira-scheduler | Trigger Spark + ML tiap 5 menit |
| aira-dashboard | Flask web dashboard |

### 5. Buat Direktori HDFS

```bash
# Tunggu namenode ready (~15 detik)
sleep 15

docker exec aira-namenode hdfs dfs -mkdir -p /aira/raw/aqi
docker exec aira-namenode hdfs dfs -mkdir -p /aira/raw/weather
docker exec aira-namenode hdfs dfs -mkdir -p /aira/raw/traffic
docker exec aira-namenode hdfs dfs -mkdir -p /aira/raw/uv
```

### 6. Tambahkan Entry Hosts

```bash
# Cek IP datanode
docker inspect aira-datanode --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}'

# Tambahkan ke /etc/hosts (ganti IP sesuai output di atas)
echo "172.19.0.6 datanode" | sudo tee -a /etc/hosts
```

### 7. Buka Dashboard

Tunggu ~2 menit hingga data pertama masuk, lalu buka:

```
http://localhost:5000
```

---

## 🔑 Konfigurasi API Key

Daftar dan dapatkan API key gratis dari:

| API | Link Daftar | Free Tier |
|---|---|---|
| AQICN | https://aqicn.org/data-platform/token/ | Unlimited |
| OpenWeatherMap | https://openweathermap.org/api | 1000 calls/hari |
| TomTom Traffic | https://developer.tomtom.com | 2500 calls/hari |
| OpenUV | https://openuv.io | 50 calls/hari |

Isi di file `.env`:

```env
AQICN_TOKEN=your_token_here
OPENWEATHER_KEY=your_key_here
TOMTOM_KEY=your_key_here
OPENUV_KEY=your_key_here
```

---

## 🌐 Akses Web UI

| Service | URL | Keterangan |
|---|---|---|
| Dashboard AIRA | http://localhost:5000 | Main dashboard |
| HDFS Web UI | http://localhost:9870 | Monitor HDFS |
| Spark Web UI | http://localhost:8080 | Monitor Spark jobs |

---

## 🔄 Pipeline Data

```
Tiap 30 detik  → Producer fetch data dari 4 API → Kafka
Tiap 30 detik  → Consumer baca Kafka → simpan ke HDFS (batch 10)
Tiap 5 menit   → Scheduler trigger Spark job
               → Spark proses data → simpan ke /aira/processed/
               → ML Serving generate prediksi → simpan ke HDFS
Tiap 60 detik  → Dashboard JS fetch data terbaru dari Flask API
```

### Cek Status Pipeline

```bash
# Cek semua container running
docker compose ps

# Cek log producer
docker logs aira-producer --tail 10

# Cek log consumer
docker logs aira-consumer --tail 10

# Cek log scheduler
docker logs aira-scheduler --tail 10

# Cek data di HDFS
docker exec aira-namenode hdfs dfs -ls -R /aira/processed/
```

### Matikan Semua Service

```bash
# Matikan container (data tetap aman)
docker compose down

# Matikan dan hapus semua data (reset total)
docker compose down -v
```

---

## 🤖 Model Machine Learning

Model **Random Forest Regressor** dilatih menggunakan dataset historis AQI Jakarta 2010–2025 dari 5 stasiun pemantau: Lubang Buaya, Jagakarsa, Kelapa Gading, Kebon Jeruk, dan Bundaran HI.

### Performa Model

| Metrik | Nilai |
|---|---|
| R² Score | **0.9802** |
| MAE | 2.5566 |
| RMSE | 5.9493 |
| Training Samples | 4,428 |
| Test Samples | 1,108 |

### Feature Importance

| Fitur | Importance |
|---|---|
| O3 (Ozon) | 81.16% |
| PM10 | 11.57% |
| Year | 4.73% |
| NO2 | 0.72% |
| CO | 0.54% |
| SO2 | 0.51% |
| Month | 0.23% |
| Is Dry Season | 0.23% |
| Stasiun | 0.18% |
| Day of Week | 0.14% |

---

## 🖥️ Tampilan
<img width="1920" height="2107" alt="screencapture-localhost-5000-2026-06-14-00_16_42" src="https://github.com/user-attachments/assets/aeebbfdd-29bc-4ee4-b3d7-35a8a23af4ee" />
