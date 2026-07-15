# GRBL WebSocket Proxy for Klipper

This project provides a lightweight WebSocket bridge that allows CNC/Laser control software (like **Candle**) to communicate with **Klipper/Moonraker**. It translates common GRBL commands and status queries into the JSON-RPC format that Moonraker understands.

> **⚠️ DISCLAIMER: PROTOTYPE NOTICE**
> This project is currently in a **non-functional/experimental state**. It serves as a structural foundation for mapping GRBL protocols to Moonraker but is not yet complete. It lacks full command validation, comprehensive error handling, and complete state synchronization. **Use at your own risk** and do not rely on it for production-critical CNC operations, as unexpected machine behavior or unhandled G-code commands may occur.

---

## Features

* **GRBL Emulation:** Basic command structure for `$`, `$$`, `$#`, `$G`, `?`, etc.
* **Moonraker Integration:** Foundation for translating G-code moves and state changes.
* **Auto-Reconnect:** Includes logic for handling connection drops between the proxy and Moonraker.
* **Dynamic Configuration:** Prototype logic for syncing machine parameters from `printer.cfg`.

---

## Setup Instructions

### 1. Prepare the Environment

It is recommended to use a Python virtual environment to manage dependencies cleanly.

```bash
# Navigate to your project directory
cd /home/user/

# Create the virtual environment
python3 -m venv grbl-bridge-env

# Activate the environment
source /home/user/grbl-bridge-env/bin/activate

# Install the necessary library
pip install websockets

```

### 2. Configure as a Systemd Service

To ensure the proxy runs in the background and starts automatically on boot, create a service file:

```bash
sudo nano /etc/systemd/system/grbl-proxy.service

```

Paste the following configuration into the file. **Note:** Update `User` and paths to match your actual system configuration.

```ini
[Unit]
Description=GRBL WebSocket Proxy for Klipper
After=network.target moonraker.service

[Service]
# Replace 'user' with your system username
User=user
WorkingDirectory=/home/user
# Point to the python binary inside your virtual environment
ExecStart=/home/user/grbl-bridge-env/bin/python /home/user/grbl-proxy.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target

```

### 3. Enable and Start the Service

Apply the new service configuration and start the proxy:

```bash
# Reload systemd to recognize the new file
sudo systemctl daemon-reload

# Enable the service to start on boot
sudo systemctl enable grbl-proxy.service

# Start the service immediately
sudo systemctl start grbl-proxy.service

```

---

## Service Management

Use these commands to monitor or update your service:

* **Check status/logs:** `sudo systemctl status grbl-proxy.service`
* **View real-time logs:** `journalctl -u grbl-proxy.service -f`
* **Restart after code changes:** `sudo systemctl restart grbl-proxy.service`

---
