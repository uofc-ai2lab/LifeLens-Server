"""
run.py — LifeLens Server Entry Point
=====================================

Starts both the MQTT receiver and the FastAPI server in a single process.

    MQTT receiver  → runs in a background thread (blocking loop_forever)
    API server     → runs in the main thread via uvicorn

Usage:
    python run.py

The two components are connected via the notify_new_data callback:
    db_writer → notify_new_data() → SSE queue → frontend
"""

import logging
import threading
import uvicorn

from subscriber.mqtt_receiver import start as start_mqtt, set_on_new_data_callback
from api.api_server import app, notify_new_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    # Wire up the SSE notification callback before starting either service
    set_on_new_data_callback(notify_new_data)
    logger.info("[Run] SSE callback registered")

    # Start MQTT receiver in a background thread
    mqtt_thread = threading.Thread(target=start_mqtt, name="MQTTReceiver", daemon=True)
    mqtt_thread.start()
    logger.info("[Run] MQTT receiver started")

    # Start API server in the main thread
    logger.info("[Run] Starting API server on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()