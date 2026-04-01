"""
image_handler.py — LifeLens Image Handler (Server side)
========================================================

Saves incoming encrypted images to the session folder.
Images are named by their image_id (the filename sent from the Jetson)
so the frontend can request a specific image by ID later.

The Jetson sends only new image_ids it hasn't sent before, so
duplicate prevention is handled on the sender side.

Files are stored as-is (Fernet-encrypted bytes). Decryption happens
in api_server.py when the image is requested by the frontend.
"""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


def handle(
    device_id: str,
    session_id: str,
    filename: str,
    file_bytes: bytes,
):
    """
    Save an encrypted image to the session folder using its original
    filename (which is the image_id) so it can be retrieved by ID later.

    Args:
        device_id:  Jetson device identifier
        session_id: Server-generated session identifier
        filename:   image_id — used as the filename on disk
        file_bytes: Raw Fernet-encrypted image bytes
    """
    try:
        session_dir = DATA_DIR / device_id / session_id
        session_dir.mkdir(parents=True, exist_ok=True)

        save_path = session_dir / filename
        with open(save_path, "wb") as f:
            f.write(file_bytes)

        logger.info(f"[Image] Saved → {save_path}")

    except Exception as e:
        logger.error(
            f"[Image] Failed to save image '{filename}' for {device_id}/{session_id}: {e}")
