"""
session_manager.py — LifeLens Session Manager (Server side)
============================================================

Tracks the single active session for the one Jetson device.

Session ID format:
    session_{YYYYMMDD}_{HHMMSS}_{device_id}
    e.g. session_20260308_143022_jetson01
"""

import logging
import threading
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

SESSION_TIMEOUT_HOURS = 3
SESSION_IDLE_TIMEOUT_SECONDS = 1200

# The single active session — just a string, no dict needed
_active_session_id: Optional[str] = None
_session_start_time: Optional[datetime] = None
_last_heartbeat_time: Optional[datetime] = None

_on_session_event = None
_lock = threading.RLock()


def set_session_event_callback(callback):
    global _on_session_event
    _on_session_event = callback


def update_heartbeat():
    global _last_heartbeat_time
    with _lock:
        _last_heartbeat_time = datetime.now()


def open_session(device_id: str) -> str:
    global _active_session_id, _session_start_time, _last_heartbeat_time
    with _lock:
        if _active_session_id:
            logger.warning(
                f"[Session] Overwriting existing session {_active_session_id}")
        _active_session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{device_id}"
        _session_start_time = datetime.now()
        _last_heartbeat_time = datetime.now()
        logger.info(f"[Session] Opened: {_active_session_id}")
        return _active_session_id


def close_session(device_id: str):
    global _active_session_id, _session_start_time, _last_heartbeat_time
    with _lock:
        if not _active_session_id:
            logger.warning(
                f"[Session] No active session to close for '{device_id}'")
            return
        session_id = _active_session_id
        _active_session_id = None
        _session_start_time = None
        _last_heartbeat_time = None
        logger.info(f"[Session] Closed: {session_id}")

    # Fire callback OUTSIDE the lock — it calls notify_new_data which puts
    # onto a queue; no risk of deadlock, and we don't want to hold the lock
    # during an external call anyway.
    if _on_session_event:
        _on_session_event(device_id, session_id, "session_end")


def get_active_device_id() -> Optional[str]:
    with _lock:
        if not _active_session_id:
            return None
        return "_".join(_active_session_id.split("_")[3:])


def get_session(device_id: str) -> Optional[str]:
    with _lock:
        if not _active_session_id:
            return None
        now = datetime.now()
        if _last_heartbeat_time:
            idle = (now - _last_heartbeat_time).total_seconds()
            if idle >= SESSION_IDLE_TIMEOUT_SECONDS:
                logger.warning(
                    f"[Session] Idle timeout ({idle:.1f}s) — auto-closing {_active_session_id}")
                close_session(device_id)  # RLock lets this re-enter safely
                return None
        if _session_start_time:
            elapsed = (now - _session_start_time).total_seconds() / 3600
            if elapsed >= SESSION_TIMEOUT_HOURS:
                logger.warning(
                    f"[Session] Max duration reached — auto-closing {_active_session_id}")
                close_session(device_id)
                return None
        return _active_session_id
