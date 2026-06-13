FROM python:3.11-slim

WORKDIR /app

# Install docker CLI supaya bisa docker exec ke namenode
RUN apt-get update && apt-get install -y \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

CMD ["python3", "-u", "consumer.py"]
