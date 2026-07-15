#!/usr/bin/env python3
"""
GRBL WebSocket Proxy for Klipper (Moonraker)
"""

import asyncio
import json
import logging
import re
from typing import Optional, Dict

import websockets
from websockets.protocol import State

# ========================= CONFIG =========================
CANDLE_HOST = "0.0.0.0"
CANDLE_PORT = 5000
MOONRAKER_WS = "ws://127.0.0.1:7125/websocket"

LOG_LEVEL = logging.INFO
STATUS_INTERVAL = 0.25
POLL_INTERVAL = 2.0
RECONNECT_DELAY = 3
MAX_SPINDLE_SPEED = 10000
# ========================================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


class KlipperGRBLBridge:
    def __init__(self):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.cmd_id = 100
        self.lock = asyncio.Lock()

        # GRBL-like state
        self.position = [0.0, 0.0, 0.0]
        self.gcode_position = [0.0, 0.0, 0.0]
        self.homing_origin = [0.0, 0.0, 0.0]
        self.g92_offset = [0.0, 0.0, 0.0]

        self.speed = 0
        self.state = "Idle"
        self.estop = False
        self.spindle_speed = 0
        self.spindle_on = False
        self.laser_on = False
        self.klipper_ready = False

        # Fake GRBL settings ($ commands)
        self.settings: Dict[str, str] = {
            "$0": "10",      # Step pulse time
            "$1": "25",      # Step idle delay
            "$2": "0",       # Step direction invert
            "$3": "0",       # Direction port invert
            "$4": "0",       # Step enable invert
            "$5": "0",       # Limit pins invert
            "$6": "0",       # Probe pin invert
            "$10": "1",      # Status report options
            "$11": "0.010",  # Junction deviation
            "$12": "0.002",  # Arc tolerance
            "$13": "0",      # Report in inches
            "$20": "0",      # Soft limits
            "$21": "0",      # Hard limits
            "$22": "1",      # Homing cycle
            "$23": "0",      # Homing direction invert
            "$24": "25.0",   # Homing feed rate
            "$25": "500.0",  # Homing seek rate
            "$26": "250",    # Homing debounce
            "$27": "1.0",    # Homing pull-off
            "$30": str(MAX_SPINDLE_SPEED),   # Max spindle speed
            "$31": "0",      # Min spindle speed
            "$32": "1",      # Laser mode (1 = Laser mode active)
            # Default fallbacks (overwritten once connected to Klipper)
            "$100": "80.000",   # X steps/mm
            "$101": "80.000",   # Y steps/mm
            "$102": "400.000",  # Z steps/mm
            "$110": "18000.000", # X max feed rate (mm/min)
            "$111": "18000.000", # Y max feed rate (mm/min)
            "$112": "900.000",   # Z max feed rate (mm/min)
            "$120": "3000.000",  # X acceleration (mm/s^2)
            "$121": "3000.000",  # Y acceleration (mm/s^2)
            "$122": "100.000",   # Z acceleration (mm/s^2)
            "$130": "220.000",  # X max travel (mm)
            "$131": "220.000",  # Y max travel (mm)
            "$132": "250.000",  # Z max travel (mm)
        }

    async def connect(self):
        """Connect to Moonraker with auto-reconnect"""
        self.klipper_ready = False
        try:
            if self.ws:
                await self.ws.close()

            logger.info("Connecting to Moonraker...")
            self.ws = await websockets.connect(MOONRAKER_WS, ping_interval=10, ping_timeout=20)
            logger.info("Connected to Moonraker")
            await self.subscribe()
            asyncio.create_task(self.poll_status())
            return
        except Exception as e:
            self.klipper_ready = False
            logger.error("Connection failed: %s", e)
            self.ws = None

    async def subscribe(self):
        subscribe_msg = {
            "jsonrpc": "2.0",
            "method": "printer.objects.subscribe",
            "params": {
                "objects": {
                    "toolhead": ["position", "status"],
                    "gcode_move": ["speed", "gcode_position"],
                    "print_stats": ["state"],
                }
            },
            "id": 1
        }
        await self.ws.send(json.dumps(subscribe_msg))
        await self.ws.recv()

        asyncio.create_task(self.status_reader())

        # Query Klipper configuration parameters immediately on start
        #await self.request_klipper_config()

    async def request_klipper_config(self):
        """Dispatches a request to fetch Klipper's active printer.cfg settings"""
        if not self.ws or self.ws.state is State.CLOSED:
            return
        logger.info("Requesting active printer.cfg config from Moonraker...")
        msg = {
            "jsonrpc": "2.0",
            "method": "printer.objects.query",
            "params": {
                "objects": {
                    "configfile": ["config"]
                }
            },
            "id": 9999  # Unique ID handled by the reader
        }
        try:
            await self.ws.send(json.dumps(msg))
        except Exception as e:
            logger.error("Failed to transmit config query: %s", e)

    def update_settings_from_config(self, config: dict):
        """
        Parses raw Klipper settings and translates them to GRBL equivalents.
        """
        try:
            def get_stepper_vals(name):
                stepper = config.get(name, {})
                return {
                    "rotation_distance": float(stepper.get("rotation_distance", 40.0)),
                    "microsteps": int(stepper.get("microsteps", 16)),
                    "step_angle": float(stepper.get("step_angle", 1.8)),
                    "position_max": float(stepper.get("position_max", 220.0)),
                }

            x_cfg = get_stepper_vals("stepper_x")
            y_cfg = get_stepper_vals("stepper_y")
            z_cfg = get_stepper_vals("stepper_z")

            printer = config.get("printer", {})
            max_vel = float(printer.get("max_velocity", 300.0))
            max_accel = float(printer.get("max_accel", 3000.0))
            max_z_vel = float(printer.get("max_z_velocity", max_vel / 20.0))
            max_z_accel = float(printer.get("max_z_accel", max_accel / 30.0))

            # Dynamic Equation: ((360 / step_angle) * microsteps) / rotation_distance
            def calc_steps_mm(cfg):
                steps_per_rev = 360.0 / cfg["step_angle"]
                return (steps_per_rev * cfg["microsteps"]) / cfg["rotation_distance"]

            # 1. Steps per mm ($100, $101, $102)
            self.settings["$100"] = f"{calc_steps_mm(x_cfg):.3f}"
            self.settings["$101"] = f"{calc_steps_mm(y_cfg):.3f}"
            self.settings["$102"] = f"{calc_steps_mm(z_cfg):.3f}"

            # 2. Maximum Feed Rates in mm/min ($110, $111, $112)
            self.settings["$110"] = f"{max_vel * 60.0:.3f}"
            self.settings["$111"] = f"{max_vel * 60.0:.3f}"
            self.settings["$112"] = f"{max_z_vel * 60.0:.3f}"

            # 3. Acceleration in mm/s^2 ($120, $121, $122)
            self.settings["$120"] = f"{max_accel:.3f}"
            self.settings["$121"] = f"{max_accel:.3f}"
            self.settings["$122"] = f"{max_z_accel:.3f}"

            # 4. Maximum Travel Limits ($130, $131, $132)
            self.settings["$130"] = f"{x_cfg['position_max']:.3f}"
            self.settings["$131"] = f"{y_cfg['position_max']:.3f}"
            self.settings["$132"] = f"{z_cfg['position_max']:.3f}"

            # 5. Homing Direction Invert Mask ($23)
            # In GRBL: Bit 0 = X, Bit 1 = Y, Bit 2 = Z. (1 = Home to MAX, 0 = Home to MIN)
            homing_mask = 0
            if float(config.get("stepper_x", {}).get("position_endstop", 0)) >= (x_cfg["position_max"] - 1.0):
                homing_mask |= 1
            if float(config.get("stepper_y", {}).get("position_endstop", 0)) >= (y_cfg["position_max"] - 1.0):
                homing_mask |= 2
            if float(config.get("stepper_z", {}).get("position_endstop", 0)) >= (z_cfg["position_max"] - 1.0):
                homing_mask |= 4
            self.settings["$23"] = str(homing_mask)

            logger.info("Successfully updated internal GRBL parameters from Klipper's active config!")
        except Exception as e:
            logger.error("Failed to compute GRBL settings from config payload: %s", e)

    async def poll_status(self):
        """Poll overall printer state"""
        while self.ws and self.ws.state is not State.CLOSED:
            try:
                msg = {
                    "jsonrpc": "2.0",
                    "method": "printer.info",
                    "id": 8888
                }
                await self.ws.send(json.dumps(msg))
                await asyncio.sleep(POLL_INTERVAL)
            except Exception:
                self.klipper_ready = False
                break

    async def status_reader(self):
        while self.ws and self.ws.state is not State.CLOSED:
            try:
                data = json.loads(await self.ws.recv())

                # Handle printer.info poll
                if data.get("id") == 8888 and "result" in data:
                    result = data["result"]
                    new_ready = result.get("state") == "ready"
                    
                    # If Klipper transitioned from unready to ready, query parameters
                    if new_ready and not self.klipper_ready:
                        asyncio.create_task(self.request_klipper_config())

                    self.klipper_ready = new_ready
                    if not self.klipper_ready:
                        logger.warning("Klipper not ready. State: %s", result.get("state"))
                    continue

                # Handle Klipper Configuration response (sent by self.request_klipper_config())
                if data.get("id") == 9999 and "result" in data:
                    try:
                        status = data["result"].get("status", {})
                        config = status.get("configfile", {}).get("config")
                        if not config: # Fallback format handler
                            config = data["result"].get("configfile", {}).get("config")
                        
                        if config:
                            self.update_settings_from_config(config)
                        else:
                            logger.error("Could not trace configfile dictionary in response data.")
                    except Exception as e:
                        logger.error("Error parsing configfile JSON: %s", e)
                    continue

                if data.get("method") != "notify_status_update":
                    continue

                update = data["params"][0]

                if "toolhead" in update:
                    pos = update["toolhead"].get("position")
                    if pos:
                        self.position = pos[:3]

                if "gcode_move" in update:
                    gcode_move = update["gcode_move"]
                    if "position" in gcode_move:
                        self.position = gcode_move["position"][:3]
                    if "gcode_position" in gcode_move:
                        self.gcode_position = gcode_move["gcode_position"][:3]
                    if "homing_origin" in gcode_move:
                        self.homing_origin = gcode_move["homing_origin"][:3]
                    if "g92_offset" in gcode_move:
                        self.g92_offset = gcode_move["g92_offset"][:3]

                    self.speed = gcode_move.get("speed", self.speed)

                if "print_stats" in update:
                    raw = update["print_stats"].get("state", "Idle")
                    self.state = self._map_state(raw)

            except Exception as e:
                self.klipper_ready = False
                logger.warning("Status reader error: %s", e)
                break

        # Connection is dead, clean up reference
        if self.ws:
            await self.ws.close()
            self.ws = None

    def get_grbl_parameters(self) -> str:
        """Return GRBL-style $# parameter report"""
        lines = ["$#"]

        # Work offsets (G54-G59). Klipper uses one main offset + G92 so we simulate this behavior.:
        x, y, z = self.homing_origin
        lines.append(f"[G54:{x:.3f},{y:.3f},{z:.3f}]")   # Most common work offset

        # Other common offsets (simplified)
        lines.append(f"[G55:0.000,0.000,0.000]")
        lines.append(f"[G56:0.000,0.000,0.000]")
        lines.append(f"[G57:0.000,0.000,0.000]")
        lines.append(f"[G58:0.000,0.000,0.000]")
        lines.append(f"[G59:0.000,0.000,0.000]")

        # G92 temporary offset
        gx, gy, gz = self.g92_offset
        lines.append(f"[G92:{gx:.3f},{gy:.3f},{gz:.3f}]")

        # Probe offset (if you use it)
        lines.append(f"[TLO:0.000]")   # Tool Length Offset
        lines.append(f"[PRB:0.000,0.000,0.000:0]")

        return "\r\n".join(lines) + "\r\nok\r\n"

    def _map_state(self, klipper_state: str) -> str:
        mapping = {
            "printing": "Run", "paused": "Hold", "complete": "Idle", "standby": "Idle", "error": "Alarm"
        }
        return mapping.get(klipper_state.lower(), klipper_state.capitalize())

    def get_grbl_status(self) -> str:
        x, y, z = self.position
        state = "Alarm" if (not self.klipper_ready or self.estop) else self.state

        spindle = self.spindle_speed if self.spindle_on else 0
        return f"<{state}|MPos:{x:.3f},{y:.3f},{z:.3f}|FS:{int(self.speed)},{spindle}>\r\n"

    async def send_gcode(self, command: str):
        if not command or self.estop or not self.ws or self.ws.state is State.CLOSED:
            logger.warning("Skipping G-code send: WebSocket closed or Emergency Stopped.")
            return
        async with self.lock:
            self.cmd_id += 1
            msg = {
                "jsonrpc": "2.0",
                "method": "printer.gcode.script",
                "params": {"script": command},
                "id": self.cmd_id
            }
            try:
                await self.ws.send(json.dumps(msg))
                # Log at INFO level so you can verify transmission in the console
                logger.info("→ Sent to Moonraker: %s", command.replace('\n', ' \\n '))
            except Exception as e:
                logger.error("Failed to transmit G-code to Moonraker: %s", e)

    async def emergency_stop(self):
        logger.critical("!!! EMERGENCY STOP !!!")
        self.estop = True
        self.spindle_on = False
        try:
            await self.send_gcode("M112")
        except Exception as e:
            logger.error("E-stop failed: %s", e)

    async def home(self):
        logger.info("Homing (G28)")
        self.state = "Home"
        await self.send_gcode("G28")

    async def status_broadcast(self, websocket):
        while True:
            try:
                await asyncio.sleep(STATUS_INTERVAL)
                await websocket.send(self.get_grbl_status().encode())
            except Exception:
                break

    async def handle_jog(self, cmd: str, websocket):
        if self.estop or self.state not in ("Idle", "Run"):
            await websocket.send(b"error:9\r\n")
            return

        try:
            jog_str = cmd[3:].strip()
            feed = 600
            move_part = re.sub(r'G[0-9]+|F[\d.]+', '', jog_str, flags=re.IGNORECASE).strip()

            match = re.search(r'F([\d.]+)', jog_str, re.IGNORECASE)
            if match:
                feed = float(match.group(1))

            if not move_part:
                await websocket.send(b"ok\r\n")
                return

            # Use newlines to separate commands so Klipper's parser reads them correctly
            gcode = f"G91\nG1 {move_part} F{feed}\nG90"
            logger.info("Jog: %s → %s", cmd, gcode.replace('\n', ' | '))
            await self.send_gcode(gcode)
            await websocket.send(b"ok\r\n")
        except Exception as e:
            logger.error("Jog failed: %s", e)
            await websocket.send(b"error:1\r\n")    

async def handle_candle(websocket, bridge: KlipperGRBLBridge):
    logger.info("Candle client connected")
    await websocket.send(b"Grbl 1.1h.2026 ['$' for help]\r\n")
    asyncio.create_task(bridge.status_broadcast(websocket))

    try:
        async for message in websocket:
            data = message if isinstance(message, bytes) else message.encode()

            remaining = bytearray()
            for b in data:
                # Realtime commands
                if b == 0x18:   # Ctrl-X
                    logger.info("Soft Reset")
                    bridge.estop = False
                    await websocket.send(b"Grbl 1.1h.2026 ['$' for help]\r\n")
                    continue
                elif b == 0x21:  # !
                    await bridge.send_gcode("PAUSE")
                    await websocket.send(b"ok\r\n")
                    continue
                elif b == 0x7E:  # ~
                    await bridge.send_gcode("RESUME")
                    await websocket.send(b"ok\r\n")
                    continue
                elif b == 0x3F:  # ?
                    await websocket.send(bridge.get_grbl_status().encode())
                    continue

                remaining.append(b)

            if not remaining:
                continue

            cmd = remaining.decode("utf-8", errors="ignore").strip()
            if not cmd:
                continue

            # Quiet down background polling logs
            if cmd in ("$G", "?"):
                logger.debug("RX (poll): %s", cmd)
            else:
                logger.info("RX: %s", cmd)

            # --- GRBL SPECIFIC INTERCEPT CHANNELS ---

            if cmd == "$":
                await websocket.send(b"[HLP:$$ $# $G $I $N $x=val $J=line $SLP $C $X $H ~ ! ? ctrl-x]\r\nok\r\n")
                continue
            
            elif cmd == "$$":
                # Clean Sort (Lexical sorting results in $100 after $10, so convert to integers)
                try:
                    sorted_settings = sorted(bridge.settings.items(), key=lambda item: int(item[0].replace("$", "")))
                except Exception:
                    sorted_settings = bridge.settings.items()

                for k, v in sorted_settings:
                    await websocket.send(f"{k}={v}\r\n".encode())
                await websocket.send(b"ok\r\n")
                continue

            elif cmd == "$G":
                # Construct a mock GRBL parser state
                # G21 = mm, G90 = absolute, G54 = coordinate system, G94 = feed per minute
                # M5 = spindle off (M3 if spindle_on), M9 = coolant off
                motion_mode = "G0"  # Or G1, but G0 is safe for idle
                coord_system = "G54"
                plane = "G17"       # XY Plane
                units = "G21"       # Metric (mm)
                distance_mode = "G90" # Absolute
                feed_mode = "G94"   # Units per minute
                
                spindle_state = "M3" if bridge.spindle_on else "M5"
                coolant_state = "M9" # Mocked off
                
                # Active feed (F) and spindle speed (S)
                feed_rate = int(bridge.speed)
                spindle_speed = bridge.spindle_speed if bridge.spindle_on else 0
                
                parser_state = (
                    f"[GC:{motion_mode} {coord_system} {plane} {units} {distance_mode} "
                    f"{feed_mode} {spindle_state} {coolant_state} T0 F{feed_rate} S{spindle_speed}]\r\n"
                )
                await websocket.send(parser_state.encode())
                await websocket.send(b"ok\r\n")
                continue

            elif cmd == "$#":
                await websocket.send(bridge.get_grbl_parameters().encode())
                continue

            elif cmd == "$H":
                logger.info("Homing requested")
                await bridge.home()
                await websocket.send(b"ok\r\n")
                continue

            elif cmd.startswith("$J="):
                logger.info("Jogging requested")
                await bridge.handle_jog(cmd, websocket)
                continue

            elif cmd == "$X":
                logger.info("Unlock requested")
                bridge.estop = False
                await websocket.send(b"ok\r\n")
                continue

            elif cmd.startswith("$"):
                logger.info("$ something requested")
                await websocket.send(b"ok\r\n")
                continue

            elif cmd.upper() == "M112":
                logger.info("Emergency Stop requested")
                await bridge.emergency_stop()
                await websocket.send(b"ALARM: Emergency stop\r\n")
                continue

            # Spindle / Laser
            elif cmd.startswith(("M3", "M4")):
                bridge.spindle_on = True
                if "S" in cmd:
                    try:
                        bridge.spindle_speed = min(int(re.search(r'S(\d+)', cmd).group(1)), MAX_SPINDLE_SPEED)
                    except Exception:
                        pass
                await bridge.send_gcode(cmd)
                await websocket.send(b"ok\r\n")
                continue

            elif cmd.startswith("M5"):
                bridge.spindle_on = False
                await bridge.send_gcode(cmd)
                await websocket.send(b"ok\r\n")
                continue

            elif re.match(r"^S\d+", cmd):
                try:
                    bridge.spindle_speed = min(int(re.search(r"S(\d+)", cmd).group(1)), MAX_SPINDLE_SPEED)
                    logger.info("Spindle speed set to %d", bridge.spindle_speed)
                    await websocket.send(b"ok\r\n")
                except Exception:
                    await websocket.send(b"ok\r\n")
                continue

            else:
                # G-code fallthrough commands
                await bridge.send_gcode(cmd)
                await websocket.send(b"ok\r\n")

    except websockets.exceptions.ConnectionClosed:
        logger.info("Candle client disconnected")
    except Exception as e:
        logger.error("Candle handler error: %s", e)

async def main():
    bridge = KlipperGRBLBridge()

    # Start the Candle WebSocket server once
    await websockets.serve(
        lambda ws: handle_candle(ws, bridge),
        CANDLE_HOST,
        CANDLE_PORT,
        ping_interval=None
    )
    logger.info(f"GRBL Proxy listening on ws://{CANDLE_HOST}:{CANDLE_PORT}")

    # Maintain Moonraker connection and status readers independently
    while True:
        await bridge.connect()
        while bridge.ws and bridge.ws.state is not State.CLOSED:
            await asyncio.sleep(1)
        
        logger.warning("Moonraker connection lost. Attempting reconnect in %ds...", RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Proxy shutting down...")
