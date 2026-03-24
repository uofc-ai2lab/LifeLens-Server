"""
db_writer.py — LifeLens SQLite Writer (Server side)
=====================================================

Responsible for:
    - Initializing the SQLite database and creating tables on first run
    - Parsing the three "frontend CSVs" (medx, intervention, visual)
    - Inserting parsed rows into the appropriate table
    - Notifying api_server.py that new data is available via a callback

Only called by csv_handler.py, and only for these three data types:
    - "medx"         → medications table
    - "intervention" → interventions table
    - "visual"       → visual_injuries table

The anonymization (transcript) CSV is NOT parsed here — it is served
as a raw file download by the API.
"""

import csv
import logging
import sqlite3
from io import StringIO
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger(__name__)

# ==========================
# CONFIG
# ==========================

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "lab_data.db"

# Data types this module handles — anything else is ignored
FRONTEND_DATA_TYPES = {"medx", "intervention"}


# ==========================
# DATABASE INIT
# ==========================

def init_db():
    """
    Create the database and tables if they don't already exist.
    Safe to call multiple times — uses CREATE TABLE IF NOT EXISTS.
    Called once at startup from mqtt_receiver.py.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS medications (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id              TEXT    NOT NULL,
            session_id             TEXT    NOT NULL,
            start_time             TEXT,
            end_time               TEXT,
            medication             TEXT,
            medication_confidence  REAL,
            dosage                 TEXT,
            dosage_confidence      REAL,
            route                  TEXT,
            route_confidence       REAL,
            created_at             DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS interventions (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id        TEXT    NOT NULL,
            session_id       TEXT    NOT NULL,
            start_time       TEXT,
            end_time         TEXT,
            event_type       TEXT,
            event_category   TEXT,
            entity_detected  TEXT,
            full_text        TEXT,
            created_at       DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()
    logger.info("[DB] Database initialized")


# ==========================
# PUBLIC ENTRY POINT
# ==========================

def write(
    device_id: str,
    session_id: str,
    data_type: str,
    csv_content: bytes,
    on_new_data: Optional[Callable] = None,
):
    """
    Parse a CSV file and insert its rows into the appropriate table.

    Args:
        device_id:    Jetson device identifier (e.g. "jetson01")
        session_id:   Server-generated session identifier
        data_type:    One of "medx", "intervention", "visual"
        csv_content:  Raw bytes of the CSV file
        on_new_data:  Optional callback fired after successful insert,
                      used by api_server.py to trigger an SSE notification.
                      Signature: on_new_data(device_id, session_id, data_type)
    """
    if data_type not in FRONTEND_DATA_TYPES:
        logger.warning(f"[DB] data_type '{data_type}' is not a frontend CSV — skipping")
        return

    try:
        text = csv_content.decode("utf-8")
        rows = list(csv.DictReader(StringIO(text)))

        if not rows:
            logger.warning(f"[DB] CSV for {data_type} is empty — nothing to insert")
            return

        if data_type == "medx":
            _insert_medications(device_id, session_id, rows)
        elif data_type == "intervention":
            _insert_interventions(device_id, session_id, rows)

        logger.info(f"[DB] Inserted {len(rows)} row(s) into {data_type} table")

        if on_new_data:
            on_new_data(device_id, session_id, data_type)

    except Exception as e:
        logger.error(f"[DB] Failed to write {data_type} data: {e}")


# ==========================
# TABLE-SPECIFIC INSERTERS
# ==========================

def _insert_medications(device_id: str, session_id: str, rows: list):
    """
    Insert rows from a medx CSV into the medications table.

    Expected CSV columns (from medX_*.csv):
        start_time, end_time, event_type,
        medication (confidence score), dosage (confidence score), route (confidence score)

    The confidence scores are embedded in the column headers in parentheses.
    We extract just the medication name and store the confidence separately.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for row in rows:
        # Column headers include confidence scores, e.g. "medication (confidence score)"
        # We find the right column flexibly rather than hardcoding the exact header string
        medication_col = _find_col(row, "medication")
        dosage_col     = _find_col(row, "dosage")
        route_col      = _find_col(row, "route")

        # Values look like: "fentanyl (0.900)" — split name from score, cast score to float
        medication, med_conf = _split_confidence(row.get(medication_col, ""))
        dosage,     dos_conf = _split_confidence(row.get(dosage_col, ""))
        route,      rou_conf = _split_confidence(row.get(route_col, ""))

        cursor.execute("""
            INSERT INTO medications
                (device_id, session_id, start_time, end_time,
                 medication, medication_confidence,
                 dosage,     dosage_confidence,
                 route,      route_confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            device_id,
            session_id,
            row.get("start_time", ""),
            row.get("end_time", ""),
            medication, _safe_float(med_conf),
            dosage,     _safe_float(dos_conf),
            route,      _safe_float(rou_conf),
        ))

    conn.commit()
    conn.close()


def _insert_interventions(device_id: str, session_id: str, rows: list):
    """
    Insert rows from an intervention CSV into the interventions table.

    Expected CSV columns (from intervention_*.csv):
        start_time, end_time, event_type, event_category,
        entity_detected, full_text
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for row in rows:
        cursor.execute("""
            INSERT INTO interventions
                (device_id, session_id, start_time, end_time,
                 event_type, event_category, entity_detected, full_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            device_id,
            session_id,
            row.get("start_time", ""),
            row.get("end_time", ""),
            row.get("event_type", ""),
            row.get("event_category", ""),
            row.get("entity_detected", ""),
            row.get("full_text", ""),
        ))

    conn.commit()
    conn.close()

# ==========================
# UTILITIES
# ==========================

def _find_col(row: dict, keyword: str) -> str:
    """
    Find a column name in a CSV row dict that contains the given keyword.
    Used to handle column headers like "medication (confidence score)"
    without hardcoding the full header string.

    Returns the matching key, or the keyword itself as a fallback.
    """
    for key in row:
        if keyword.lower() in key.lower():
            return key
    return keyword


def _split_confidence(value: str) -> tuple:
    """
    Split a value like "fentanyl (0.900)" into ("fentanyl", "0.900").
    If no confidence score is present, returns (value, "").
    """
    if " (" in value and value.endswith(")"):
        name, score = value.rsplit(" (", 1)
        return name.strip(), score.rstrip(")")
    return value.strip(), ""


def _safe_float(value: str) -> Optional[float]:
    """Convert a string to float, returning None if conversion fails."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return None