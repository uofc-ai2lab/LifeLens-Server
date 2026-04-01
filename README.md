# LifeLens Server

Receives data from Jetson devices over a private Tailscale network via MQTT, and serves it to the frontend via a FastAPI REST + SSE API.

## Prerequisites

- [Mosquitto](https://mosquitto.org/download/) installed and on your PATH
- [Tailscale](https://tailscale.com/) running and authenticated to the same tailnet as the Jetson devices
- Python 3.11+
- Dependencies installed (see [Setup](#setup))

## Setup

```bash
python -m venv venv
venv\Scripts\activate       # Windows
# source venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```
 
## Configuration

Create a `.env` file in the project root:

```env
# Comma-separated username:password pairs for frontend login
LIFELENS_USERS=<username1>:<password1>,<username2>:<password2>,...

# Fernet key used to decrypt images captured by the Jetson
IMAGE_DECRYPT_KEY=<your-fernet-key> # Must be the same key used to encrypt on the Jetson device

# Password required by the AHS portal to decrypt images
AHS_PASSWORD=<ahs-portal-password> # One can set this to whatever one wishes.

# The following variables must also be set. 
MQTT_BROKER=<ip=address-of-server> # Tailscale IP address of server running Mosquitto broker
MQTT_PORT=1883 # port set in Mosquitto config on server
MQTT_USERNAME=<mosquitto_config_username>
MQTT_PASSWORD=<mosquitto_config_password>

# Tailscale generated SSL certificate used to allow communication between HTTPS front-end and HTTP backend. Please see the next section on how this is set up. 
SSL_CERTIFILE=<path-to-ssl-certifile>
SSL_KEYFILE=<path-to-ssl-keyfile>
```

## Certifications (Tailscale)

The frotend is runs on HTTPS while our backend currently runs via HTTP. Due to this mismatch, called 'mixed content', the
two cannot communicate as HTTPS pages will block requests to the HTTP backend. We get around this by utilizing Tailscale's built
in HTTPS functionality that gives our backend server machine (which is connected to Tailscale) an HTTPS endpoint that can be connected to, but still
have the luxury not being publicly exposed to the public internet.

To set up, ensure that in the server machine is connected to the Tailscale network, then ensure HTTPS is enabled:
- Go to https://login.tailscale.com/admin/dns
- Scroll down to HTTPS Certificates and click Enable
- Also make sure MagicDNS is enabled on the same page (required for cert to work)

Then in the terminal run `tailscale cert <machine-name>.<tailnet-name>.ts.net`. You can find the exact domain/name by running `tailscale status`.
Two files will be a generated. a .crt file and a .key file. Move both to wherever you wish, and copy the paths to those files to SSL_CERTIFILE, 
and SSL_KEYFILE in the .env file.

## Running

**Step 1 — Start the MQTT broker**

```bash
mosquitto -v -c "C:\Users\Ai_user\LifeLens\LifeLens-Server\config\secure.conf"
```

**Step 2 — Start the server** (new terminal)

```bash
python run.py
```

This starts the MQTT receiver and the API server together. The API is available at `https://0.0.0.0:8000`.

## Further Readings/Explanations
Please find any further explanations of how the server + frontend work in the file docstrings and comments.