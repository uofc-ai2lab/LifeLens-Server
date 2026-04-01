"""
json_handler.py — LifeLens JSON Handler (Server side)
======================================================

Responsible for saving visual_output.json to the session folder.
Unlike CSVs, this file is always overwritten — the Jetson sends a
cumulative "best seen so far" state each time, so the latest file
is always the correct one.

Called by router.py for data_type == "visual".
"""

import json
import logging
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def handle(
    device_id: str,
    session_id: str,
    file_bytes: bytes,
    on_new_data: Optional[Callable] = None,
):
    """
    Save visual_output.json to disk and notify the frontend.

    Args:
        device_id:   Jetson device identifier
        session_id:  Server-generated session identifier
        file_bytes:  Raw bytes of the JSON file
        on_new_data: Optional SSE notification callback
    """
    try:
        # Validate it's parseable JSON before saving
        json.loads(file_bytes.decode("utf-8"))

        session_dir = DATA_DIR / device_id / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        save_path = session_dir / "visual_output.json"
        with open(save_path, "wb") as f:
            f.write(file_bytes)

        logger.info(f"[JSON] Saved visual_output.json → {save_path}")

        if on_new_data:
            on_new_data(device_id, session_id, "visual")

    except Exception as e:
        logger.error(
            f"[JSON] Failed to save visual data for {device_id}/{session_id}: {e}")
