To start up the LifeLens Server (Spins up Tailscale service, which enables this PC to connect as a 'reciever' and other registered Tailscale devices),
run `mosquitto -v -c "C:\Users\Ai_user\LifeLens\LifeLens-Server\config\secure.conf"`

Then, open a new terminal and run `python app.py` to start the MQTT reciever and Python Flask Service that enables the frontend to be connected. 
