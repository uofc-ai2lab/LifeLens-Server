import os
import json
import base64
import sqlite3
from pathlib import Path
import paho.mqtt.client as mqtt

# ==========================
# CONFIG
# ==========================

BROKER = "100.77.50.93"   # Tailscale IP
PORT = 1883
USERNAME = "Jetson"
PASSWORD = "Secure123"

BASE_DIR = Path(__file__).resolve().parent.parent
INCOMING_DIR = BASE_DIR / "data" / "incoming"
AUDIO_DIR = BASE_DIR / "data" / "audio"
VIDEO_DIR = BASE_DIR / "data" / "video"
DB_PATH = BASE_DIR / "db" / "lab_data.db"

TOPIC = "lab/ingest/#"

# ==========================
# DATABASE SETUP
# ==========================

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT,
            file_type TEXT,
            filename TEXT,
            content BLOB
        )
    """)

    conn.commit()
    conn.close()

# ==========================
# MQTT CALLBACKS
# ==========================

def on_connect(client, userdata, flags, rc):
    print("Connected with result code", rc)
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        device_id = payload["device_id"]
        filename = payload["filename"]
        index = payload["index"]
        total = payload["total"]
        file_bytes = base64.b64decode(payload["bytes_b64"])

        # Extract file type from topic
        # lab/ingest/audio/device01
        parts = msg.topic.split("/")
        file_type = parts[2]  # audio or video or csv

        device_folder = INCOMING_DIR / device_id / filename
        device_folder.mkdir(parents=True, exist_ok=True)

        chunk_path = device_folder / f"{index}.chunk"

        with open(chunk_path, "wb") as f:
            f.write(file_bytes)

        print(f"Received chunk {index+1}/{total} for {filename}")

        # Check if complete
        received_chunks = list(device_folder.glob("*.chunk"))
        if len(received_chunks) == total:
            print(f"All chunks received for {filename}")
            rebuild_file(device_id, filename, total, file_type)

    except Exception as e:
        print("Error:", e)

# ==========================
# FILE REBUILD
# ==========================

def rebuild_file(device_id, filename, total, file_type):
    device_folder = INCOMING_DIR / device_id / filename
    final_data = b""

    for i in range(total):
        chunk_path = device_folder / f"{i}.chunk"
        with open(chunk_path, "rb") as f:
            final_data += f.read()

    # Choose destination
    if file_type == "audio":
        save_path = AUDIO_DIR / f"{device_id}_{filename}"
    elif file_type == "video":
        save_path = VIDEO_DIR / f"{device_id}_{filename}"
    else:
        save_path = BASE_DIR / "data" / f"{device_id}_{filename}"

    with open(save_path, "wb") as f:
        f.write(final_data)

    print(f"Rebuilt file saved to {save_path}")

    insert_into_db(device_id, file_type, filename, final_data.decode(errors="ignore"))

    # Cleanup
    for file in device_folder.glob("*.chunk"):
        file.unlink()
    device_folder.rmdir()

# ==========================
# INSERT INTO SQLITE
# ==========================

def insert_into_db(device_id, file_type, filename, content):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO measurements (device_id, file_type, filename, content)
        VALUES (?, ?, ?, ?)
    """, (device_id, file_type, filename, content))

    conn.commit()
    conn.close()

    print(f"Inserted {filename} into database")

# ==========================
# MAIN
# ==========================

if __name__ == "__main__":
    init_db()

    client = mqtt.Client()
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT, 60)
    client.loop_forever()
