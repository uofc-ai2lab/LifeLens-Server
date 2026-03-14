"""
csv_handler.py — LifeLens CSV Handler (Server side)
=====================================================

Responsible for:
    - Saving all incoming CSV files to the correct session folder on disk
    - For the three "frontend CSVs", also calling db_writer to parse
      and insert rows into SQLite

Frontend CSVs (parsed into DB):
    - "medx"         → saved as medx.csv         + inserted into medications table
    - "intervention" → saved as intervention.csv  + inserted into interventions table
    - "visual"       → saved as visual.csv        + inserted into visual_injuries table

Non-frontend CSV (saved to disk only):
    - "anonymization" → saved as anonymization.csv (served as raw file download)

Since the same data_type can arrive multiple times per session (one per audio chunk),
CSV rows are APPENDED to the existing file rather than overwriting it.
The DB inserter handles this naturally — each call just inserts new rows.
"""

import logging
from pathlib import Path
from typing import Callable, Optional

from . import db_writer
from dotenv import load_dotenv
load_dotenv()  # loads .env into environment before anything else runs

logger = logging.getLogger(__name__)

# ==========================
# CONFIG
# ==========================

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# These data types get saved to disk AND inserted into the database
FRONTEND_DATA_TYPES = {"medx", "intervention", "visual"}

# Canonical filenames on the server — we don't trust incoming filenames for naming
FILENAME_MAP = {
    "anonymization": "anonymization.csv",
    "medx":          "medx.csv",
    "intervention":  "intervention.csv",
    "visual":        "visual.csv",
}


# ==========================
# PUBLIC ENTRY POINT
# ==========================

def handle(
    device_id: str,
    session_id: str,
    data_type: str,
    filename: str,
    file_bytes: bytes,
    on_new_data: Optional[Callable] = None,
):
    """
    Save a CSV file to disk and, if it's a frontend CSV, write it to the database.

    Args:
        device_id:    Jetson device identifier (e.g. "jetson01")
        session_id:   Server-generated session identifier
        data_type:    One of "anonymization", "medx", "intervention", "visual"
        filename:     Original filename from sender (used only for logging)
        file_bytes:   Raw decoded bytes of the CSV file
        on_new_data:  Optional callback passed through to db_writer for SSE
                      notifications. Signature: on_new_data(device_id, session_id, data_type)
    """
    try:
        session_dir = _get_session_dir(device_id, session_id)
        save_path   = _get_save_path(session_dir, data_type, filename)

        _append_csv(save_path, file_bytes)
        logger.info(f"[CSV] Saved → {save_path}")

        # Parse and insert into DB for frontend-facing data types
        if data_type in FRONTEND_DATA_TYPES:
            db_writer.write(
                device_id=device_id,
                session_id=session_id,
                data_type=data_type,
                csv_content=file_bytes,
                on_new_data=on_new_data,
            )

    except Exception as e:
        logger.error(f"[CSV] Failed to handle {data_type} for {device_id}/{session_id}: {e}")


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


def _get_save_path(session_dir: Path, data_type: str, original_filename: str) -> Path:
    """
    Return the canonical save path for this data type.

    We use a fixed filename per data type (from FILENAME_MAP) rather than
    the original filename — the incoming filename is unreliable and irrelevant
    for server-side organization. The topic already tells us exactly what the
    file is via data_type.

    Falls back to the original filename only if the data_type is unrecognized.
    """
    canonical = FILENAME_MAP.get(data_type)
    if canonical:
        return session_dir / canonical

    # Unrecognized data type — use original name as a safe fallback
    logger.warning(f"[CSV] Unrecognized data_type '{data_type}', using original filename")
    return session_dir / original_filename


def _append_csv(save_path: Path, file_bytes: bytes):
    """
    Append CSV rows to an existing file, or create it if it doesn't exist yet.

    On the first write, the full file (including header) is written.
    On subsequent writes (same data_type arriving again in the same session),
    the header row is stripped before appending to avoid duplicate headers.
    """
    # Normalise to \n so the file is consistent regardless of platform
    text = file_bytes.decode("utf-8")
    lines = text.splitlines()

    if not lines:
        return

    if save_path.exists():
        lines_to_write = lines[1:]
        if not lines_to_write:
            return
        with open(save_path, "ab") as f:
            f.write(("\n".join(lines_to_write) + "\n").encode("utf-8"))
        logger.info(f"[CSV] Appended {len(lines_to_write)} row(s) to {save_path.name}")
    else:
        with open(save_path, "wb") as f:
            f.write(("\n".join(lines) + "\n").encode("utf-8"))
        logger.info(f"[CSV] Created {save_path.name} with {len(lines) - 1} row(s)")