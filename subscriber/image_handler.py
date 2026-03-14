"""
image_handler.py — LifeLens Image Handler (Server side)
========================================================

Responsible for:
    - Decoding a base64-encoded image from the MQTT payload
    - Determining the next available sequence number for this session
    - Saving the image to the correct path under the session folder

File naming convention:
    data/raw/{device_id}/{session_id}/image_001.jpg
    data/raw/{device_id}/{session_id}/image_002.jpg
    ...

Multiple images can arrive per session (one per AprilTag detection trigger
on the video pipeline), so each is saved with a zero-padded sequence number
rather than a fixed name. No images are ever overwritten.
"""

import base64
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ==========================
# CONFIG
# ==========================

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Supported image extensions — used to preserve the original file extension
SUPPORTED_EXTENSIONS = {".jpg", ".jpeg", ".png"}
DEFAULT_EXTENSION = ".jpg"


# ==========================
# PUBLIC ENTRY POINT
# ==========================

def handle(
    device_id: str,
    session_id: str,
    filename: str,
    file_bytes: bytes,
):
    """
    Save a decoded image to the correct session folder with a sequence number.

    Args:
        device_id:  Jetson device identifier (e.g. "jetson01")
        session_id: Server-generated session identifier
        filename:   Original filename from the sender (used only for extension)
        file_bytes: Raw decoded bytes of the image file
    """
    try:
        session_dir = _get_session_dir(device_id, session_id)
        save_path   = _get_next_image_path(session_dir, filename)

        with open(save_path, "wb") as f:
            f.write(file_bytes)

        logger.info(f"[Image] Saved → {save_path}")

    except Exception as e:
        logger.error(f"[Image] Failed to save image for {device_id}/{session_id}: {e}")


# ==========================
# INTERNAL HELPERS
# ==========================

def _get_session_dir(device_id: str, session_id: str) -> Path:
    """
    Resolve and create the session directory if it doesn't exist.
    Path: data/raw/{device_id}/{session_id}/
    """
    session_dir = DATA_DIR / device_id / session_id
    session_dir.mkdir(parents=True, exist_ok=True)
    return session_dir


def _get_next_image_path(session_dir: Path, original_filename: str) -> Path:
    """
    Determine the next available sequenced image filename in the session folder.

    Scans existing image files to find the highest current sequence number,
    then returns a path for the next one.

    Example:
        Existing: image_001.jpg, image_002.jpg
        Returns:  image_003.jpg
    """
    # Preserve the original extension if it's a known image type
    suffix = Path(original_filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        suffix = DEFAULT_EXTENSION

    # Find the highest existing sequence number
    existing = list(session_dir.glob(f"image_*{suffix}"))
    next_index = len(existing) + 1

    return session_dir / f"image_{next_index:03d}{suffix}"