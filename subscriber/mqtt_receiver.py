"""
mqtt_receiver.py — LifeLens MQTT Receiver (Server side, entry point)
=====================================================================

This is the entry point for the server-side data ingestion pipeline.
It is intentionally thin — its only responsibilities are:

    1. Connect to the MQTT broker
    2. Subscribe to all relevant topics
    3. Decode incoming JSON payloads
    4. Hand off to router.py — nothing else

All routing, file handling, and database logic lives in the other modules.

To run:
    python -m subscriber.mqtt_receiver

Topics subscribed:
    lab/session/#      → session start/end messages
    lab/ingest/#       → data batch messages (audio + video)
"""

import json
import logging
import logging.config

import paho.mqtt.client as mqtt

from . import router
from . import db_writer
from . import session_manager

# ==========================
# LOGGING
# ==========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ==========================
# CONFIG
# ==========================

BROKER = "100.77.50.93"   # Tailscale IP (matches secure.conf)
PORT = 1883
USERNAME = "Jetson"
PASSWORD = "Secure123"

# Subscribe to all session and ingest topics in one wildcard each
TOPICS = [
    ("lab/session/#", 1),
    ("lab/ingest/#",  1),
    ("lab/heartbeat/#", 1)
]


# ==========================
# SSE NOTIFICATION CALLBACK
# ==========================

# This callback is set by api_server.py at startup so the receiver can
# notify connected frontend clients when new data arrives in the database.
# If api_server.py is not running, it stays None and notifications are skipped.
_on_new_data_callback = None


def set_on_new_data_callback(callback):
    """
    Register the SSE notification callback from api_server.py.

    Args:
        callback: Callable with signature (device_id, session_id, data_type)
    """
    global _on_new_data_callback
    _on_new_data_callback = callback
    logger.info("[Receiver] SSE notification callback registered")


# ==========================
# MQTT CALLBACKS
# ==========================

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"[Receiver] Connected to broker at {BROKER}:{PORT}")
        # Subscribe inside on_connect so subscriptions are restored automatically
        # if the client reconnects after a dropped connection
        for topic, qos in TOPICS:
            client.subscribe(topic, qos)
            logger.info(f"[Receiver] Subscribed to: {topic}")
    else:
        logger.error(f"[Receiver] Connection refused, return code: {rc}")


def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(
            f"[Receiver] Unexpected disconnect (rc={rc}) — paho will retry")
    else:
        logger.info("[Receiver] Clean disconnect")


def on_message(client, userdata, msg):
    """
    Called by paho for every incoming message.
    Decode the JSON payload and pass to router — nothing else.
    """
    topic = msg.topic

    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logger.error(
            f"[Receiver] Failed to decode message on topic '{topic}': {e}")
        return

    logger.debug(f"[Receiver] Message on '{topic}'")

    # Handle heartbeat messages (liveness tracking)
    if topic.startswith("lab/heartbeat/"):
        session_manager.update_heartbeat()
        return  # Do NOT route further

    parts = topic.split("/")
    is_session_start = (
        len(parts) == 4 and parts[1] == "session" and parts[2] == "start"
    )

    router.route(
        topic=topic,
        payload=payload,
        on_new_data=_on_new_data_callback,
    )

    # Forward session_start as an SSE event so the frontend can navigate to
    # the new session. session_end is already fired inside
    # session_manager.close_session() — firing it here too would double-send.
    if _on_new_data_callback and is_session_start:
        device_id = payload.get("device_id", parts[3])
        session_id = session_manager.get_session(device_id) or ""
        _on_new_data_callback(device_id, session_id, "session_start")


# ==========================
# MAIN
# ==========================

def start():
    """
    Initialize the database and start the MQTT listener.
    Blocks forever via loop_forever().
    """
    logger.info("[Receiver] Initializing database...")
    db_writer.init_db()

    client = mqtt.Client(client_id="lifelens-server")
    client.username_pw_set(USERNAME, PASSWORD)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.connect(BROKER, PORT, keepalive=60)

    logger.info("[Receiver] Starting MQTT listener...")
    client.loop_forever()   # Blocks here — handles reconnects automatically


if __name__ == "__main__":
    start()