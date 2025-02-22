import logging
import threading
import time
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify

from app_config import AppConfig
from loader.processor import DdsMessageProcessor
from loader.repository import DdsRepository

# Initialize Flask app
app = Flask(__name__)

# Configure logger
logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

# Initialize config
config = AppConfig()

# Initialize processor
proc = DdsMessageProcessor(
    config.kafka_consumer(),
    config.kafka_producer(),
    DdsRepository(config.pg_warehouse_db()),
    config.DEFAULT_BATCH_SIZE,
    log
)

# Initialize the scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
scheduler.start()

# Flag to monitor worker process health
worker_healthy = True


def worker_monitor():
    """Background monitor to check if the worker process is running properly."""
    global worker_healthy
    while True:
        try:
            # If the scheduler is running, mark worker as healthy
            worker_healthy = scheduler.running
        except Exception as e:
            log.error(f"Worker monitor error: {e}")
            worker_healthy = False
        time.sleep(10)


# Start the worker health monitor in a separate thread
monitor_thread = threading.Thread(target=worker_monitor, daemon=True)
monitor_thread.start()


@app.get("/health")
def health():
    """Health check endpoint that verifies both API and worker process."""
    return jsonify({"service":"dds_service",
        "api": "ok",
        "worker": "ok" if worker_healthy else "not running"
    })


if __name__ == "__main__":
    try:
        log.info("Starting dds_service...")
        app.run(debug=True, host="0.0.0.0", port=5003, use_reloader=False)
    except KeyboardInterrupt:
        log.info("Shutting down service...")
        scheduler.shutdown()
        log.info("Scheduler stopped.")