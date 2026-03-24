"""
api_server.py — LifeLens API Server (Server side)
==================================================

Endpoints:
    POST /login                                     → authenticate, receive token
    GET  /sessions                                  → list recent sessions (auth required)
    GET  /sessions/active                           → current active session or null
    GET  /sessions/{session_id}/medications         → medication rows
    GET  /sessions/{session_id}/interventions       → intervention rows
    GET  /sessions/{session_id}/transcript          → raw CSV download
    GET  /sessions/{session_id}/images              → list image filenames
    GET  /sessions/{session_id}/images/{filename}   → serve image file
    GET  /events                                    → SSE stream (auth required)

Authentication:
    Users are defined in the server's .env file as:
        LIFELENS_USERS=alice:password1,bob:password2

    Login returns a token (UUID). All protected endpoints require:
        Authorization: Bearer <token>

    Tokens are stored in memory — they expire when the server restarts.
    This is intentional for a small internal tool.
"""

import asyncio
import logging
import os
import queue
import sqlite3
import uuid
from pathlib import Path
import threading
from typing import AsyncGenerator
from datetime import datetime
from subscriber.session_manager import get_active_device_id, get_session


from fastapi import Depends, FastAPI, Header, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ==========================
# CONFIG
# ==========================

DB_PATH = Path(__file__).resolve().parent.parent / "db" / "lab_data.db"
DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


# ==========================
# APP SETUP
# ==========================

app = FastAPI(title="LifeLens API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ==========================
# AUTHENTICATION
# ==========================

# Load users from environment variable at startup
# Format: LIFELENS_USERS=alice:pass1,bob:pass2


def _load_users() -> dict:
    raw = os.getenv("LIFELENS_USERS", "")
    users = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" in entry:
            username, password = entry.split(":", 1)
            users[username.strip()] = password.strip()
    return users


USERS = _load_users()

# In-memory token store: { token: username }
_active_tokens: dict = {}

security = HTTPBearer()


def require_auth(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    FastAPI dependency — validates the Bearer token on protected endpoints.
    Add `auth=Depends(require_auth)` to any endpoint to protect it.
    """
    token = credentials.credentials
    if token not in _active_tokens:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    return _active_tokens[token]


class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/login")
def login(body: LoginRequest):
    """
    Authenticate with username and password.
    Returns a token to use as a Bearer token on all subsequent requests.

    Credentials are read from the LIFELENS_USERS environment variable.
    """
    expected_password = USERS.get(body.username)
    if not expected_password or body.password != expected_password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials",
        )
    token = str(uuid.uuid4())
    _active_tokens[token] = body.username
    logger.info(f"[Auth] User '{body.username}' logged in")
    return {"token": token, "username": body.username}


# ==========================
# SSE NOTIFICATION QUEUE
# ==========================

_sse_clients: list[queue.Queue] = []
_sse_clients_lock = threading.Lock()


def notify_new_data(device_id: str, session_id: str, data_type: str):
    """
    Called by db_writer.py whenever new rows are inserted.
    Also called by mqtt_receiver.py for session_start / session_end events.

    This function is called from the MQTT thread, not the async event loop,
    so we use queue.Queue (thread-safe) rather than asyncio.Queue.
    """
    event = {"device_id": device_id,
             "session_id": session_id, "data_type": data_type}
    with _sse_clients_lock:
        for client_queue in _sse_clients:
            client_queue.put_nowait(event)   # every client gets every event

# ==========================
# SSE ENDPOINT
# ==========================


def require_auth_sse(token: str = None, credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=False))):
    """
    Auth dependency for the SSE endpoint only.
    Accepts token as a query parameter because EventSource does not
    support custom headers — Bearer token cannot be sent the normal way.
    """
    # Try query param first (EventSource), then Authorization header (regular requests)
    raw_token = token or (credentials.credentials if credentials else None)
    if not raw_token or raw_token not in _active_tokens:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="Invalid or expired token")
    return _active_tokens[raw_token]


@app.get("/events")
async def events(_: str = Depends(require_auth_sse)):
    """
    SSE stream. The frontend opens this once and keeps it alive.

    Event data_type values:
        "medx"          → new medication rows available
        "intervention"  → new intervention rows available
        "visual"        → new injury rows available
        "session_start" → a new live session has begun
        "session_end"   → the active session has ended
    """
    return StreamingResponse(
        _sse_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


async def _sse_generator() -> AsyncGenerator[str, None]:
    import json
    client_queue = queue.Queue()
    with _sse_clients_lock:
        _sse_clients.append(client_queue)    # register on connect
    try:
        loop = asyncio.get_event_loop()
        while True:
            try:
                event = await loop.run_in_executor(
                    None, lambda: client_queue.get(timeout=15.0)
                )
                yield f"data: {json.dumps(event)}\n\n"
            except queue.Empty:
                yield ": heartbeat\n\n"
    finally:
        with _sse_clients_lock:
            _sse_clients.remove(client_queue)  # clean up on disconnect

# ==========================
# SESSION ENDPOINTS
# ==========================


@app.get("/sessions/active")
def get_active_session(_: str = Depends(require_auth)):
    """
    Returns the current active session, or null if none is running.

    The home screen calls this on load to show a Live Session banner.

    Returns:
        { "session_id": "session_20260308_143022_jetson01", "device_id": "jetson01" }
        or
        { "session_id": null, "device_id": null }
    """
    device_id = get_active_device_id()
    if not device_id:
        return {"session_id": None, "device_id": None}
    session_id = get_session(device_id)
    if session_id:
        return {"session_id": session_id, "device_id": device_id}
    return {"session_id": None, "device_id": None}


@app.get("/sessions")
def get_sessions(_: str = Depends(require_auth)):
    """
    List the 20 most recent sessions, most recent first.

    Returns:
        [
            {
                "session_id": "session_20260308_143022_jetson01",
                "device_id":  "jetson01",
                "created_at": "2026-03-08 14:30:22"
            },
            ...
        ]
    """
    sessions = []
    if DATA_DIR.exists():
        for device_dir in sorted(DATA_DIR.iterdir()):
            if not device_dir.is_dir():
                continue
            for session_dir in sorted(device_dir.iterdir()):
                if not session_dir.is_dir():
                    continue
                session_id = session_dir.name
                device_id = device_dir.name
                try:
                    parts = session_id.split("_")
                    created_at = datetime.strptime(
                        f"{parts[1]} {parts[2]}", "%Y%m%d %H%M%S"
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except (IndexError, ValueError):
                    created_at = ""
                sessions.append({
                    "session_id": session_id,
                    "device_id":  device_id,
                    "created_at": created_at,
                })

    sessions.sort(key=lambda s: s["created_at"], reverse=True)
    return sessions[:20]


# ==========================
# DATA ENDPOINTS
# ==========================

@app.get("/sessions/{session_id}/medications")
def get_medications(session_id: str, _: str = Depends(require_auth)):
    conn = _get_db()
    rows = conn.execute("""
        SELECT id, start_time, end_time,
               medication, medication_confidence,
               dosage,     dosage_confidence,
               route,      route_confidence,
               created_at
        FROM medications
        WHERE session_id = ?
        ORDER BY start_time
    """, (session_id,)).fetchall()
    conn.close()
    if not rows:
        _raise_if_session_unknown(session_id)
    return [_row_to_dict(row, [
        "id", "start_time", "end_time",
        "medication", "medication_confidence",
        "dosage",     "dosage_confidence",
        "route",      "route_confidence",
        "created_at",
    ]) for row in rows]


@app.get("/sessions/{session_id}/interventions")
def get_interventions(session_id: str, _: str = Depends(require_auth)):
    conn = _get_db()
    rows = conn.execute("""
        SELECT id, start_time, end_time,
               event_type, event_category, entity_detected, full_text,
               created_at
        FROM interventions
        WHERE session_id = ?
        ORDER BY start_time
    """, (session_id,)).fetchall()
    conn.close()
    if not rows:
        _raise_if_session_unknown(session_id)
    return [_row_to_dict(row, [
        "id", "start_time", "end_time",
        "event_type", "event_category", "entity_detected", "full_text",
        "created_at",
    ]) for row in rows]

# ==========================
# FILE ENDPOINTS
# ==========================


@app.get("/sessions/{session_id}/transcript")
def get_transcript(session_id: str, device_id: str, _: str = Depends(require_auth)):
    path = DATA_DIR / device_id / session_id / "anonymization.csv"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Transcript not found")
    return FileResponse(path=str(path), media_type="text/csv",
                        filename=f"transcript_{session_id}.csv")


@app.get("/sessions/{session_id}/visual")
def get_visual(session_id: str, device_id: str, _: str = Depends(require_auth)):
    """
    Return the latest visual_output.json for a session.
    The frontend uses this to drive the body map injury visualization.
    """
    path = DATA_DIR / device_id / session_id / "visual_output.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Visual data not found")
    return FileResponse(path=str(path), media_type="application/json")


@app.get("/sessions/{session_id}/images/{image_id}")
def get_image_encrypted(session_id: str, image_id: str, device_id: str,
                        _: str = Depends(require_auth)):
    """
    Serve the raw Fernet-encrypted image bytes.
    The frontend renders these as pixel noise to show the anonymized state.
    """
    path = DATA_DIR / device_id / session_id / image_id
    if not path.exists():
        raise HTTPException(status_code=404, detail="Image not found")
    return Response(content=path.read_bytes(), media_type="application/octet-stream")


@app.get("/sessions/{session_id}/images")
def list_images(session_id: str, device_id: str, _: str = Depends(require_auth)):
    """
    List all image IDs available for a session.
    Returns filenames only — use /images/{image_id} to fetch each one.
    """
    session_dir = DATA_DIR / device_id / session_id
    if not session_dir.exists():
        raise HTTPException(status_code=404, detail="Session not found")

    # Exclude the CSV and JSON data files — only return image files
    excluded = {".csv", ".json"}
    images = [
        p.name for p in sorted(session_dir.iterdir())
        if p.is_file() and p.suffix not in excluded
    ]
    return {"images": images}


@app.post("/sessions/{session_id}/images/{image_id}/decrypt")
def get_image_decrypted(session_id: str, image_id: str, device_id: str,
                        _: str = Depends(require_auth)):
    """
    Decrypt and serve an image for authenticated medical staff.
    Requires a valid Bearer token (standard login).
    Used by the body map hover in the session view.
    """
    return _decrypt_and_serve(session_id, image_id, device_id)


@app.post("/sessions/{session_id}/images/{image_id}/decrypt-ahs")
def get_image_decrypted_ahs(
    session_id: str,
    image_id: str,
    device_id: str,
    ahs_password: str = Header(..., alias="AHS-Password"),
):
    """
    Decrypt and serve an image for AHS portal users.
    Requires the AHS-Password header instead of a Bearer token.
    The AHS password is stored in .env as AHS_PASSWORD.
    """
    expected = os.getenv("AHS_PASSWORD", "")
    if not expected or ahs_password != expected:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid AHS password",
        )
    return _decrypt_and_serve(session_id, image_id, device_id)


def _decrypt_and_serve(session_id: str, image_id: str, device_id: str):
    """
    Shared decryption logic used by both decrypt endpoints.
    Reads IMAGE_DECRYPT_KEY from .env, decrypts the stored image, returns JPEG bytes.
    """
    from cryptography.fernet import Fernet

    decrypt_key = os.getenv("IMAGE_DECRYPT_KEY", "")
    if not decrypt_key:
        raise HTTPException(
            status_code=500, detail="IMAGE_DECRYPT_KEY not configured")

    path = DATA_DIR / device_id / session_id / image_id
    if not path.exists():
        raise HTTPException(status_code=404, detail="Image not found")

    try:
        fernet = Fernet(decrypt_key.encode())
        encrypted_bytes = path.read_bytes()
        decrypted_bytes = fernet.decrypt(encrypted_bytes)
    except Exception as e:
        logger.error(f"[API] Image decryption failed for {image_id}: {e}")
        raise HTTPException(status_code=500, detail="Image decryption failed")

    return Response(content=decrypted_bytes, media_type="image/jpeg")


# ==========================
# UTILITIES
# ==========================

def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: sqlite3.Row, keys: list) -> dict:
    return {key: row[key] for key in keys}


def _raise_if_session_unknown(session_id: str):
    conn = _get_db()
    exists = conn.execute("""
        SELECT 1 FROM (
            SELECT session_id FROM medications     WHERE session_id = ?
            UNION
            SELECT session_id FROM interventions   WHERE session_id = ?
            UNION
            SELECT session_id FROM visual_injuries WHERE session_id = ?
        ) LIMIT 1
    """, (session_id, session_id, session_id)).fetchone()
    conn.close()
    if not exists:
        raise HTTPException(
            status_code=404, detail=f"Session '{session_id}' not found")
