FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY aira_rf_model.pkl .
COPY aira_le_stasiun.pkl .
COPY feature_importance.json .
COPY model_metrics.json .
COPY ml_serving.py .

CMD ["python3", "-u", "ml_serving.py"]
