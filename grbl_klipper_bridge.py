#!/usr/bin/env python3
"""
GRBL 1.1h ↔ Klipper Bridge v2
"""

import asyncio
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set

try:
    import websockets
    from websockets.asyncio.server import serve
except Exception:  # pragma: no cover - provide clearer error when dependency missing
    raise RuntimeError(
        "The 'websockets' package is required. Install it with: pip install websockets"
        )


# ========================= CONFIG =========================
MOONRAKER_HOST = "localhost"
MOONRAKER_PORT = 7125
GRBL_WS_PORT = 8080
STATUS_INTERVAL = 0.12
RPC_TIMEOUT = 5.0
COMMAND_QUEUE_SIZE = 256
# =======================================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class GRBLSettings:
    steps_per_mm: Dict[str, float] = field(default_factory=lambda: {"x": 80.0, "y": 80.0, "z": 400.0})
    max_rate: Dict[str, float] = field(default_factory=lambda: {"x": 5000.0, "y": 5000.0, "z": 300.0})
    accel: Dict[str, float] = field(default_factory=lambda: {"x": 500.0, "y": 500.0, "z": 100.0})
    max_travel: Dict[str, float] = field(default_factory=lambda: {"x": 200.0, "y": 200.0, "z": 200.0})


@dataclass
class ModalState:
    motion_mode: str = "G0"
    distance_mode: str = "G90"
    plane: str = "G17"
    units: str = "G21"
    coord_system: str = "G54"
    feed_mode: str = "G94"
    spindle_mode: str = "M5"
    coolant_mode: str = "M9"
    tool: str = "T0"
    feed_rate: float = 0.0
    spindle_speed: int = 0

    def as_grbl_string(self) -> str:
        return (
            f"[GC:{self.motion_mode} {self.coord_system} {self.plane} {self.units} "
            f"{self.distance_mode} {self.feed_mode} {self.spindle_mode} {self.coolant_mode} "
            f"{self.tool} F{int(self.feed_rate)} S{self.spindle_speed}]"
        )


class StatusCache:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.mpos: list[float] = [0.0, 0.0, 0.0]
        self.wpos: list[float] = [0.0, 0.0, 0.0]
        self.feed_speed: float = 0.0
        self.spindle: int = 0
        self.homed_axes: str = ""
        self.grbl_state: str = "Idle"
        self.gcode_move: Dict = {}
        self.toolhead: Dict = {}
        self.print_stats_state: str = "Idle"
        self.idle_timeout_state: str = "Idle"

    async def update(self, status: Dict):
        async with self.lock:
            if "toolhead" in status:
                self.toolhead = status["toolhead"]
                self.mpos = self.toolhead.get("position", self.mpos)[:3]
                self.homed_axes = self.toolhead.get("homed_axes", "")
            if "gcode_move" in status:
                self.gcode_move = status["gcode_move"]
                self.feed_speed = self.gcode_move.get("speed", self.feed_speed)
                homing_origin = self.gcode_move.get("homing_origin", [0] * 3)[:3]
                g92_offset = self.gcode_move.get("g92_offset", [0] * 3)[:3]
                self.wpos = [m - ho - g92 for m, ho, g92 in zip(self.mpos, homing_origin, g92_offset)]
            if "print_stats" in status:
                self.print_stats_state = status["print_stats"].get("state", "Idle")
            if "idle_timeout" in status:
                self.idle_timeout_state = status["idle_timeout"].get("state", "Idle")
            self._derive_grbl_state()

    def _derive_grbl_state(self):
        ps = self.gcode_move.get("state", "") or self.toolhead.get("status", "") or self.print_stats_state or self.idle_timeout_state
        ps_lower = (ps or "").lower()
        if self.idle_timeout_state.lower() in {"printing", "busy"} or "printing" in ps_lower or ps_lower == "busy":
            self.grbl_state = "Run"
        elif "paused" in ps_lower or "hold" in ps_lower:
            self.grbl_state = "Hold"
        elif not self.homed_axes:
            self.grbl_state = "Alarm" if self.grbl_state == "Idle" else self.grbl_state
        else:
            self.grbl_state = "Idle"

    def get_status(self) -> str:
        return f"<{self.grbl_state}|MPos:{','.join(f'{x:.3f}' for x in self.mpos)}|WPos:{','.join(f'{x:.3f}' for x in self.wpos)}|FS:{int(self.feed_speed)},{self.spindle}>"


class RPCDispatcher:
    def __init__(self):
        self.pending: Dict[int, asyncio.Future] = {}
        self._id = 0
        self.lock = asyncio.Lock()

    async def request(self, ws, method: str, params: Optional[Dict] = None, timeout: float = RPC_TIMEOUT) -> Any:
        if not ws:
            raise ConnectionError("Moonraker not connected")
        async with self.lock:
            self._id = (self._id + 1) % 999999
            rid = self._id
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self.pending[rid] = fut
            await ws.send(json.dumps({"jsonrpc": "2.0", "method": method, "params": params or {}, "id": rid}))
            try:
                return await asyncio.wait_for(fut, timeout)
            finally:
                self.pending.pop(rid, None)


class MoonrakerClient:
    def __init__(self, status_cache: StatusCache, bridge: Optional[Any] = None):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.rpc = RPCDispatcher()
        self.connected = asyncio.Event()
        self.status_cache = status_cache
        self.bridge = bridge

    async def connect_loop(self):
        while True:
            try:
                self.connected.clear()
                self.rpc.pending.clear()
                self.ws = await websockets.connect(f"ws://{MOONRAKER_HOST}:{MOONRAKER_PORT}/websocket", ping_interval=10)
                logger.info("Connected to Moonraker")
                self.connected.set()
                await self._post_connect()
                async for message in self.ws:
                    await self._handle_msg(message)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Moonraker disconnected: {e}")
                self.connected.clear()
                await asyncio.sleep(1.5)

    async def _post_connect(self):
        await self.rpc.request(self.ws, "printer.objects.subscribe", {
            "objects": {
                "toolhead": ["position", "homed_axes", "status"],
                "gcode_move": ["speed", "gcode_position", "homing_origin", "g92_offset"],
                "print_stats": ["state"],
                "idle_timeout": ["state"],
            }
        })

    async def _handle_msg(self, raw):
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            data = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(data, bytes):
                data = json.loads(data.decode())
            if "id" in data and data["id"] in self.rpc.pending:
                fut = self.rpc.pending.pop(data["id"])
                if "result" in data:
                    fut.set_result(data["result"])
                else:
                    fut.set_exception(Exception(str(data.get("error"))))
            elif data.get("method") == "notify_status_update":
                for update in data.get("params", []):
                    await self.status_cache.update(update)
        except Exception as e:
            logger.debug(f"Msg error: {e}")


class CommandQueue:
    def __init__(self, moonraker, bridge):
        self.moonraker = moonraker
        self.bridge = bridge
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=COMMAND_QUEUE_SIZE)

    async def worker(self):
        while True:
            cmd, client_ws = await self.queue.get()
            try:
                if self.moonraker.connected.is_set():
                    await self.moonraker.rpc.request(self.moonraker.ws, "printer.gcode.script", {"script": cmd})
                await self.bridge.safe_send(client_ws, b"ok")
            except Exception as e:
                await self.bridge.safe_send(client_ws, f"error: {e}".encode())
            finally:
                self.queue.task_done()

    async def send_now(self, gcode: str):
        """Bypass queue for emergencies."""
        if self.moonraker.connected.is_set():
            await self.moonraker.rpc.request(self.moonraker.ws, "printer.gcode.script", {"script": gcode})


class GRBLMachine:
    def __init__(self, status_cache: StatusCache, queue: CommandQueue, moonraker: MoonrakerClient, bridge):
        self.status = status_cache
        self.queue = queue
        self.moonraker = moonraker
        self.bridge = bridge
        self.settings = GRBLSettings()
        self.parser_state = ModalState()

    async def handle(self, line: str, client_ws):
        if isinstance(line, (bytes, bytearray)):
            buffer = bytearray()
            for b in line:
                if b in {0x18, 0x21, 0x7E, 0x3F}:
                    if buffer:
                        await self._handle_text(buffer.decode("utf-8", errors="ignore"), client_ws)
                        buffer.clear()
                    await self._handle_realtime_byte(b, client_ws)
                else:
                    buffer.append(b)
            if buffer:
                await self._handle_text(buffer.decode("utf-8", errors="ignore"), client_ws)
            return

        if isinstance(line, str):
            await self._handle_text(line, client_ws)
            return

    async def _handle_text(self, text: str, client_ws):
        for raw_line in re.split(r"[\r\n]+", text):
            line = raw_line.strip()
            if not line:
                continue
            await self._handle_line(line, client_ws)

    async def _handle_line(self, line: str, client_ws):
        if line == "?":
            await self.bridge.safe_send(client_ws, self.status.get_status())
            return
        if line == "!":
            await self.queue.send_now("PAUSE")
            await self.bridge.safe_send(client_ws, b"ok\r\n")
            return
        if line == "~":
            await self.queue.send_now("RESUME")
            await self.bridge.safe_send(client_ws, b"ok\r\n")
            return
        if line == "\x18":
            await self._soft_reset(client_ws)
            return
        if line.upper() == "M112":
            await self._emergency(client_ws)
            return

        if line.startswith("$"):
            await self._handle_system(line, client_ws)
            return

        self._update_parser_state(line)
        await self.queue.queue.put((line, client_ws))

    async def _handle_realtime_byte(self, b: int, client_ws):
        if b == 0x18:  # Ctrl-X
            await self._soft_reset(client_ws)
        elif b == 0x21:  # !
            await self.queue.send_now("PAUSE")
            await self.bridge.safe_send(client_ws, b"ok\r\n")
        elif b == 0x7E:  # ~
            await self.queue.send_now("RESUME")
            await self.bridge.safe_send(client_ws, b"ok\r\n")
        elif b == 0x3F:  # ?
            await self.bridge.safe_send(client_ws, self.status.get_status())

    async def _handle_system(self, cmd: str, client_ws):
        upper = cmd.upper()
        if upper == "$$":
            await self._dump_settings(client_ws)
            return
        if upper == "$G":
            await self.bridge.safe_send(client_ws, self.parser_state.as_grbl_string() + "\r\n" + "ok")
            return
        if upper == "$H":
            await self.queue.send_now("G28")
            await self.bridge.safe_send(client_ws, b"ok")
            return
        if upper.startswith("$J="):
            await self._handle_jog(upper[3:], client_ws)
            return
        if upper in {"$I", "$N", "$C"}:
            await self.bridge.safe_send(client_ws, b"ok")
            return
        if upper == "$SLP":
            await self.queue.send_now("M400")
            await self.bridge.safe_send(client_ws, b"ok")
            return
        await self.bridge.safe_send(client_ws, b"ok")

    async def _handle_jog(self, jog_str: str, client_ws):
        x_match = re.search(r'X\s*([-+]?\d*\.?\d+)', jog_str, re.IGNORECASE)
        y_match = re.search(r'Y\s*([-+]?\d*\.?\d+)', jog_str, re.IGNORECASE)
        z_match = re.search(r'Z\s*([-+]?\d*\.?\d+)', jog_str, re.IGNORECASE)
        f_match = re.search(r'F\s*(\d*\.?\d+)', jog_str, re.IGNORECASE)

        g1_parts = []
        if x_match:
            g1_parts.append(f"X{x_match.group(1)}")
        if y_match:
            g1_parts.append(f"Y{y_match.group(1)}")
        if z_match:
            g1_parts.append(f"Z{z_match.group(1)}")

        feed = f"F{f_match.group(1)}" if f_match else "F600"

        if not g1_parts:
            await self.bridge.safe_send(client_ws, b"ok")
            return

        g1_cmd = f"G1 {' '.join(g1_parts)} {feed}"

        is_absolute = "G90" in jog_str.upper()
        if is_absolute:
            script = f"G90\n{g1_cmd}"
        else:
            script = f"G91\n{g1_cmd}\nG90"

        # Update modal state for the local UI and dispatch
        self._update_parser_state(script)
        await self.queue.send_now(script)
        await self.bridge.safe_send(client_ws, b"ok")

    async def _soft_reset(self, client_ws):
        await self.queue.send_now("M112")
        await self.queue.send_now("FIRMWARE_RESTART")
        self.status.grbl_state = "Alarm"
        await self.bridge.safe_send(client_ws, b"ALARM: Emergency stop\r\n")

    async def _emergency(self, client_ws):
        await self.queue.send_now("M112")
        self.status.grbl_state = "Alarm"
        await self.bridge.safe_send(client_ws, b"ok")

    async def _dump_settings(self, client_ws):
        lines = [f"${k}={v}" for k, v in {
            100: self.settings.steps_per_mm["x"],
            101: self.settings.steps_per_mm["y"],
            102: self.settings.steps_per_mm["z"],
            110: self.settings.max_rate["x"],
            111: self.settings.max_rate["y"],
            112: self.settings.max_rate["z"],
        }.items()]
        await self.bridge.safe_send(client_ws, "\n".join(lines) + "\nok")

    def update_settings_from_config(self, config: Dict[str, Any]):
        try:
            def get_stepper_vals(name: str) -> Dict[str, float]:
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

            def calc_steps_mm(cfg: Dict[str, float]) -> float:
                steps_per_rev = 360.0 / cfg["step_angle"]
                return (steps_per_rev * cfg["microsteps"]) / cfg["rotation_distance"]

            self.settings.steps_per_mm["x"] = calc_steps_mm(x_cfg)
            self.settings.steps_per_mm["y"] = calc_steps_mm(y_cfg)
            self.settings.steps_per_mm["z"] = calc_steps_mm(z_cfg)
            self.settings.max_rate["x"] = max_vel * 60.0
            self.settings.max_rate["y"] = max_vel * 60.0
            self.settings.max_rate["z"] = max_z_vel * 60.0
            self.settings.accel["x"] = max_accel
            self.settings.accel["y"] = max_accel
            self.settings.accel["z"] = max_z_accel
            self.settings.max_travel["x"] = x_cfg["position_max"]
            self.settings.max_travel["y"] = y_cfg["position_max"]
            self.settings.max_travel["z"] = z_cfg["position_max"]
        except Exception as e:
            logger.debug(f"Settings update failed: {e}")

    def _update_parser_state(self, line: str):
        if not line:
            return
        tokens = re.findall(r"([A-Za-z])([+-]?\d*\.?\d*)", line)
        for letter, value in tokens:
            upper_letter = letter.upper()
            if upper_letter == "G":
                number = int(float(value)) if value else 0
                if number in {0, 1, 2, 3}:
                    self.parser_state.motion_mode = f"G{number}"
                elif number in {17, 18, 19}:
                    self.parser_state.plane = f"G{number}"
                elif number in {20, 21}:
                    self.parser_state.units = f"G{number}"
                elif number in {90, 91}:
                    self.parser_state.distance_mode = f"G{number}"
                elif number in {93, 94}:
                    self.parser_state.feed_mode = f"G{number}"
                elif number in {54, 55, 56, 57, 58, 59}:
                    self.parser_state.coord_system = f"G{number}"
            elif upper_letter == "M":
                number = int(float(value)) if value else 0
                if number in {3, 4, 5}:
                    self.parser_state.spindle_mode = f"M{number}"
                elif number in {7, 8, 9}:
                    self.parser_state.coolant_mode = f"M{number}"
            elif upper_letter == "F":
                if value:
                    self.parser_state.feed_rate = float(value)
            elif upper_letter == "S":
                if value:
                    self.parser_state.spindle_speed = int(float(value))
            elif upper_letter == "T":
                self.parser_state.tool = f"T{value}" if value else "T0"


class Bridge:
    def __init__(self):
        self.status_cache = StatusCache()
        self.moonraker = MoonrakerClient(self.status_cache, self)
        self.queue = CommandQueue(self.moonraker, self)
        self.grbl = GRBLMachine(self.status_cache, self.queue, self.moonraker, self)
        self.candle_clients: Set = set()
        self.client_locks: Dict[Any, asyncio.Lock] = {}
        self.status_task: Optional[asyncio.Task] = None
        self.moonraker_task: Optional[asyncio.Task] = None
        self.queue_task: Optional[asyncio.Task] = None

    async def safe_send(self, ws, data: Any):
        if not ws:
            return
        payload = data if isinstance(data, (bytes, bytearray)) else str(data).encode()
        lock = self.client_locks.get(ws)
        if lock is None:
            await ws.send(payload)
            return
        async with lock:
            await ws.send(payload)

    async def candle_handler(self, ws):
        self.candle_clients.add(ws)
        self.client_locks[ws] = asyncio.Lock()
        logger.info("Candle connected")
        try:
            await self.safe_send(ws, "Grbl 1.1h ['$' for help]\r\n")
            async for msg in ws:
                await self.grbl.handle(msg, ws)
        finally:
            self.candle_clients.discard(ws)
            self.client_locks.pop(ws, None)

    async def status_loop(self):
        while True:
            await asyncio.sleep(STATUS_INTERVAL)
            if self.candle_clients:
                report = self.status_cache.get_status()
                for c in list(self.candle_clients):
                    try:
                        await self.safe_send(c, report)
                    except Exception:
                        pass


async def main():
    bridge = Bridge()

    bridge.moonraker_task = asyncio.create_task(bridge.moonraker.connect_loop())
    bridge.queue_task = asyncio.create_task(bridge.queue.worker())
    bridge.status_task = asyncio.create_task(bridge.status_loop())

    try:
        async with serve(bridge.candle_handler, "0.0.0.0", GRBL_WS_PORT):
            logger.info(f"GRBL Bridge listening on ws://0.0.0.0:{GRBL_WS_PORT}")
            await asyncio.Future()
    except KeyboardInterrupt:
        logger.info("Shutting down bridge")
    finally:
        for task in (bridge.moonraker_task, bridge.queue_task, bridge.status_task):
            if task:
                task.cancel()
        await asyncio.gather(*(task for task in (bridge.moonraker_task, bridge.queue_task, bridge.status_task) if task), return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())