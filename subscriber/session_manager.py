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
SESSION_IDLE_TIMEOUT_SECONDS = 30

# The single active session — just a string, no dict needed
_active_session_id: Optional[str] = None
_session_start_time: Optional[datetime] = None
_last_heartbeat_time: Optional[datetime] = None

_on_session_event = None 

def set_session_event_callback(callback):
    global _on_session_event
    _on_session_event = callback

def update_heartbeat():
    global _last_heartbeat_time
    _last_heartbeat_time = datetime.now()

def open_session(device_id: str) -> str:
    global _active_session_id, _session_start_time, _last_heartbeat_time

    if _active_session_id:
        logger.warning(f"[Session] Overwriting existing session {_active_session_id}")

    _active_session_id  = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{device_id}"
    _session_start_time = datetime.now()
    _last_heartbeat_time = datetime.now()

    logger.info(f"[Session] Opened: {_active_session_id}")
    return _active_session_id


def close_session(device_id: str):
    global _active_session_id, _session_start_time, _last_heartbeat_time

    if not _active_session_id:
        logger.warning(f"[Session] No active session to close for '{device_id}'")
        return

    session_id = _active_session_id  # capture before clearing
    logger.info(f"[Session] Closed: {_active_session_id}")
    _active_session_id  = None
    _session_start_time = None
    _last_heartbeat_time = None

    if _on_session_event:
        _on_session_event(device_id, session_id, "session_end")


def get_session(device_id: str) -> Optional[str]:
    """
    Return the active session ID, or None if no session is open.
    Also auto-closes if the session has exceeded the timeout
    or if heartbeat has stopped.
    """
    if not _active_session_id:
        return None

    now = datetime.now()

    if _last_heartbeat_time:
        idle = (now - _last_heartbeat_time).total_seconds()
        if idle >= SESSION_IDLE_TIMEOUT_SECONDS:
            logger.warning(f"[Session] Idle timeout ({idle:.1f}s) — auto-closing {_active_session_id}")
            close_session(device_id)
            return None

    if _session_start_time:
        elapsed = (now - _session_start_time).total_seconds() / 3600
        if elapsed >= SESSION_TIMEOUT_HOURS:
            logger.warning(f"[Session] Max duration reached — auto-closing {_active_session_id}")
            close_session(device_id)
            return None

    return _active_session_id