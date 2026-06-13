import schedule
import subprocess
import time
from datetime import datetime

SPARK_JOB_PATH  = "/opt/spark_job.py"
ML_SERVING_PATH = "/ml-serving/venv/bin/python3"
ML_SCRIPT_PATH  = "/ml-serving/ml_serving.py"
INTERVAL_MINUTES = 5

spark_running = False

def run_spark_job():
    global spark_running
    if spark_running:
        print(f"[{datetime.utcnow().isoformat()}] Spark job still running, skipping...")
        return

    spark_running = True
    print(f"[{datetime.utcnow().isoformat()}] Running Spark job...")
    try:
        result = subprocess.run(
            [
                "docker", "exec", "aira-spark-master",
                "/spark/bin/spark-submit",
                "--master", "local[2]",
                "--driver-memory", "512m",
                "--conf", "spark.hadoop.fs.defaultFS=hdfs://namenode:9000",
                "--conf", "spark.ui.enabled=false",
                SPARK_JOB_PATH
            ],
            capture_output=True, text=True, timeout=240
        )
        if result.returncode == 0:
            print(f"[{datetime.utcnow().isoformat()}] ✓ Spark job completed!")
        else:
            print(f"[{datetime.utcnow().isoformat()}] ✗ Spark job failed!")
            print(result.stderr[-300:])
    except subprocess.TimeoutExpired:
        print(f"[{datetime.utcnow().isoformat()}] ✗ Spark job timeout!")
    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ✗ Spark error: {e}")
    finally:
        spark_running = False

def run_ml_serving():
    print(f"[{datetime.utcnow().isoformat()}] Running ML serving...")
    try:
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "--network", "aira-project_aira-net",
                "-v", "/var/run/docker.sock:/var/run/docker.sock",
                "aira-project-ml-serving",
                "python3", "-u", "ml_serving.py"
            ],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0:
            print(f"[{datetime.utcnow().isoformat()}] ✓ ML serving completed!")
            print(result.stdout[-300:])
        else:
            print(f"[{datetime.utcnow().isoformat()}] ✗ ML serving failed!")
            print(result.stderr[-300:])
    except Exception as e:
        print(f"[{datetime.utcnow().isoformat()}] ✗ ML error: {e}")

def run_pipeline():
    run_spark_job()
    run_ml_serving()

def main():
    print(f"[INFO] Scheduler started — interval: {INTERVAL_MINUTES} minutes")
    run_pipeline()
    schedule.every(INTERVAL_MINUTES).minutes.do(run_pipeline)
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == "__main__":
    main()
