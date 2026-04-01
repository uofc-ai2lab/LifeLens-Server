import os

BROKER = os.getenv("MQTT_BROKER")
PORT = int(os.getenv("MQTT_PORT", 1883))
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")

if not USERNAME or not PASSWORD or not BROKER:
    print("MQTT credentials not set in environment. Data transfer will fail.")

SSL_CERTIFILE = os.getenv("SSL_CERTIFILE")
SSL_KEYFILE = os.getenv("SSL_KEYFILE")

if not SSL_KEYFILE or not SSL_CERTIFILE:
    print("Tailscale HTTPS files are not in env file. Data transfer will not work.")
