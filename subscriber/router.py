"""
router.py — LifeLens MQTT Message Router (Server side)
=======================================================

Responsible for:
    - Receiving a decoded MQTT message from mqtt_receiver.py
    - Routing session control messages (start/end) to session_manager.py
    - Routing data messages to the correct handler (csv or image)
    - Decoding base64 file bytes before passing to handlers

This module knows nothing about MQTT itself — it receives plain Python
dicts and calls the appropriate handler. Keeps mqtt_receiver.py clean.

Topic structure:
    lab/session/start/{device_id}     → session_manager.open_session()
    lab/session/end/{device_id}       → session_manager.close_session()
    lab/ingest/audio/{device_id}      → batch of audio CSVs
    lab/ingest/video/{device_id}      → visual CSV + image

Data types by pipeline:
    audio: anonymization, medx, intervention  (all CSVs)
    video: visual (CSV), image (image file)
"""

import base64
import logging
from typing import Callable, Optional

from . import csv_handler
from . import image_handler
from . import session_manager

logger = logging.getLogger(__name__)

# Data types that are image files rather than CSVs
IMAGE_DATA_TYPES = {"image"}


# ==========================
# PUBLIC ENTRY POINT
# ==========================

def route(
    topic: str,
    payload: dict,
    on_new_data: Optional[Callable] = None,
):
    """
    Route an incoming MQTT message to the correct handler.

    Args:
        topic:        The MQTT topic string (e.g. "lab/ingest/audio/jetson01")
        payload:      The decoded JSON payload as a Python dict
        on_new_data:  Optional SSE notification callback, passed through to
                      db_writer via csv_handler.
    """
    parts = topic.split("/")

    # Validate minimum topic depth
    if len(parts) < 3:
        logger.warning(f"[Router] Unrecognized topic format: {topic}")
        return

    namespace = parts[1]  # "session" or "ingest"

    if namespace == "session":
        _route_session(parts, payload)

    elif namespace == "ingest":
        _route_ingest(parts, payload, on_new_data)

    else:
        logger.warning(f"[Router] Unknown topic namespace '{namespace}': {topic}")


# ==========================
# ROUTING BRANCHES
# ==========================

def _route_session(parts: list, payload: dict):
    """
    Handle session lifecycle messages.

    Topics:
        lab/session/start/{device_id}
        lab/session/end/{device_id}
    """
    if len(parts) < 4:
        logger.warning(f"[Router] Malformed session topic: {'/'.join(parts)}")
        return

    action    = parts[2]  # "start" or "end"
    device_id = parts[3]

    if action == "start":
        session_manager.open_session(device_id)
    elif action == "end":
        session_manager.close_session(device_id)
    else:
        logger.warning(f"[Router] Unknown session action '{action}'")


def _route_ingest(parts: list, payload: dict, on_new_data: Optional[Callable]):
    """
    Handle incoming data batch messages.

    Topics:
        lab/ingest/audio/{device_id}
        lab/ingest/video/{device_id}

    Payload contains a "files" list — each entry is routed individually
    to either csv_handler or image_handler based on its data_type.
    """
    if len(parts) < 4:
        logger.warning(f"[Router] Malformed ingest topic: {'/'.join(parts)}")
        return

    device_id = parts[3]

    # Look up the active session for this device
    session_id = session_manager.get_session(device_id)
    if not session_id:
        # Safety fallback — auto-open a session if data arrives without one
        logger.warning(
            f"[Router] No active session for '{device_id}' — "
            f"auto-opening session as fallback"
        )
        session_id = session_manager.open_session(device_id)

    files = payload.get("files", [])
    if not files:
        logger.warning(f"[Router] Ingest message from '{device_id}' has no files")
        return

    logger.info(f"[Router] Routing batch of {len(files)} file(s) for {device_id}/{session_id}")

    for file_entry in files:
        _dispatch_file(device_id, session_id, file_entry, on_new_data)


def _dispatch_file(
    device_id: str,
    session_id: str,
    file_entry: dict,
    on_new_data: Optional[Callable],
):
    """
    Decode a single file entry from the batch payload and send to the
    appropriate handler.

    Expected file_entry structure:
        {
            "data_type": "medx",
            "filename":  "medX_recording_....csv",
            "bytes_b64": "<base64 string>"
        }
    """
    data_type = file_entry.get("data_type", "")
    filename  = file_entry.get("filename", "unknown")
    bytes_b64 = file_entry.get("bytes_b64", "")

    if not data_type:
        logger.warning(f"[Router] File entry missing 'data_type' — skipping")
        return

    if not bytes_b64:
        logger.warning(f"[Router] File entry for '{data_type}' has no bytes — skipping")
        return

    # Decode base64 bytes once here — handlers receive raw bytes
    try:
        file_bytes = base64.b64decode(bytes_b64)
    except Exception as e:
        logger.error(f"[Router] Failed to decode base64 for '{data_type}': {e}")
        return

    if data_type in IMAGE_DATA_TYPES:
        image_handler.handle(
            device_id=device_id,
            session_id=session_id,
            filename=filename,
            file_bytes=file_bytes,
        )
    else:
        csv_handler.handle(
            device_id=device_id,
            session_id=session_id,
            data_type=data_type,
            filename=filename,
            file_bytes=file_bytes,
            on_new_data=on_new_data,
        )