from celery import Celery
import time
import os

# Connect to Redis (replace with your Render Redis URL later)
celery = Celery("tasks", broker=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))

@celery.task
def process_pdfs_task(file_paths):
    for path in file_paths:
        # Simulate PDF processing
        print(f"Processing: {path}")
        time.sleep(2)  # Simulate a time-consuming task
