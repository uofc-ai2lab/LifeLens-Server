"""
session_manager.py — LifeLens Session Manager (Server side)
============================================================

Tracks the single active session for the one Jetson device.

Session ID format:
    session_{YYYYMMDD}_{HHMMSS}_{device_id}
    e.g. session_20260308_143022_jetson01
"""

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

SESSION_TIMEOUT_HOURS = 3

# The single active session — just a string, no dict needed
_active_session_id: Optional[str] = None
_session_start_time: Optional[datetime] = None


def open_session(device_id: str) -> str:
    global _active_session_id, _session_start_time

    if _active_session_id:
        logger.warning(f"[Session] Overwriting existing session {_active_session_id}")

    _active_session_id  = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{device_id}"
    _session_start_time = datetime.now()

    logger.info(f"[Session] Opened: {_active_session_id}")
    return _active_session_id


def close_session(device_id: str):
    global _active_session_id, _session_start_time

    if not _active_session_id:
        logger.warning(f"[Session] No active session to close for '{device_id}'")
        return

    logger.info(f"[Session] Closed: {_active_session_id}")
    _active_session_id  = None
    _session_start_time = None


def get_session(device_id: str) -> Optional[str]:
    """
    Return the active session ID, or None if no session is open.
    Also auto-closes if the session has exceeded the timeout.
    """
    if _active_session_id and _session_start_time:
        elapsed_hours = (datetime.now() - _session_start_time).total_seconds() / 3600
        if elapsed_hours >= SESSION_TIMEOUT_HOURS:
            logger.warning(f"[Session] Timeout reached — auto-closing {_active_session_id}")
            close_session(device_id)
            return None

    return _active_session_id