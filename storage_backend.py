"""
StorageGuard — Real SD Card Reliability Backend
=================================================
Usage:
  python storage_backend.py                     # auto-detect SD card
  python storage_backend.py --path E:\\          # specify SD card drive
  python storage_backend.py --path E:\\ --port 8765

Streams real I/O latency data to the dashboard via WebSocket.
"""

import asyncio
import json
import math
import os
import random
import string
import subprocess
import sys
import tempfile
import time
import argparse
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("StorageGuard")

# ─────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────
NUM_BLOCKS         = 32
BLOCK_SIZE_BYTES   = 64 * 1024      # 64 KB per logical block
HISTORY_LEN        = 60
TICK_INTERVAL_S    = 0.6
ELEVATED_THRESHOLD = 0.40
CRITICAL_THRESHOLD = 0.70


# ─────────────────────────────────────────────────────
# SD CARD AUTO-DETECTION  (Windows + Linux/macOS)
# ─────────────────────────────────────────────────────
def detect_sd_card() -> Optional[str]:
    """Return the mount path of the first removable/SD drive found, or None."""
    if sys.platform == "win32":
        try:
            import ctypes
            drives = []
            bitmask = ctypes.windll.kernel32.GetLogicalDrives()
            for letter in string.ascii_uppercase:
                if bitmask & 1:
                    drive = f"{letter}:\\"
                    dtype = ctypes.windll.kernel32.GetDriveTypeW(drive)
                    # 2 = DRIVE_REMOVABLE
                    if dtype == 2:
                        drives.append(drive)
                bitmask >>= 1
            if drives:
                log.info(f"Detected removable drives: {drives}")
                return drives[0]
        except Exception as e:
            log.warning(f"Drive detection failed: {e}")
    else:
        # Linux / macOS: look for /media or /Volumes mounts
        candidates = []
        for base in ["/media", "/Volumes", "/mnt"]:
            if os.path.isdir(base):
                for name in os.listdir(base):
                    p = os.path.join(base, name)
                    if os.path.ismount(p):
                        candidates.append(p)
        if candidates:
            log.info(f"Detected removable mounts: {candidates}")
            return candidates[0]
    return None


def choose_path(user_path: Optional[str]) -> str:
    if user_path:
        if not os.path.exists(user_path):
            log.error(f"Path not found: {user_path}")
            sys.exit(1)
        return os.path.join(user_path, "storageguard_data")

    detected = detect_sd_card()
    if detected:
        log.info(f"Using SD card: {detected}")
        return os.path.join(detected, "storageguard_data")

    # Fallback: temp dir (real OS I/O, just not on SD card)
    tmp = tempfile.mkdtemp(prefix="storageguard_")
    log.warning(f"No SD card detected — using temp dir: {tmp}")
    log.warning("Insert your SD card and restart to measure real card latency.")
    return tmp


# ─────────────────────────────────────────────────────
# DATA MODELS
# ─────────────────────────────────────────────────────
@dataclass
class BlockState:
    block_id: int
    latency_history: deque = field(default_factory=lambda: deque(maxlen=HISTORY_LEN))
    access_count: int = 0
    write_count: int = 0
    read_count: int = 0
    is_avoided: bool = False
    degradation_injected: bool = False

    @property
    def avg_latency(self) -> float:
        return sum(self.latency_history) / len(self.latency_history) if self.latency_history else 0.0

    @property
    def latency_variance(self) -> float:
        h = list(self.latency_history)
        if len(h) < 2:
            return 0.0
        m = sum(h) / len(h)
        return sum((x - m) ** 2 for x in h) / len(h)

    @property
    def recent_trend(self) -> float:
        h = list(self.latency_history)
        if len(h) < 4:
            return 0.0
        half = len(h) // 2
        older = sum(h[:half]) / half
        newer = sum(h[half:]) / (len(h) - half)
        return (newer - older) / max(older, 1e-9)

    @property
    
    def risk_score(self) -> float:
        h = list(self.latency_history)

    if len(h) < 6:
        return 0.0

    # 🔥 Baseline = older half
    half = len(h) // 2
    baseline = sum(h[:half]) / max(half, 1)

    # 🔥 Recent behavior
    recent = h[half:]
    recent_avg = sum(recent) / max(len(recent), 1)

    # 🔥 Deviation ratio (core idea)
    deviation = recent_avg / max(baseline, 1e-6)

    # 🔥 Variability
    variance = math.sqrt(self.latency_variance)

    # 🔥 Normalize (adaptive, not fixed)
    dev_score = min((deviation - 1.0) / 2.0, 1.0) if deviation > 1 else 0.0
    var_score = min(variance / (baseline + 1e-6), 1.0)

    # 🔥 Trend already computed
    trend_score = max(min(self.recent_trend, 1.0), 0.0)

    # 🔥 Weighted score
    score = (
        0.5 * dev_score +     # deviation is most important
        0.3 * var_score +     # instability
        0.2 * trend_score     # increasing degradation
    )

    return round(min(max(score, 0.0), 1.0), 4)

    @property
    def classification(self) -> str:
        if self.is_avoided:
            return "AVOIDED"
        s = self.risk_score
        if s >= CRITICAL_THRESHOLD:
            return "CRITICAL"
        if s >= ELEVATED_THRESHOLD:
            return "ELEVATED"
        return "NORMAL"


# ─────────────────────────────────────────────────────
# REAL I/O TELEMETRY
# ─────────────────────────────────────────────────────
class Telemetry:
    def __init__(self, base_path: str):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self._buf = os.urandom(BLOCK_SIZE_BYTES)
        log.info(f"I/O working directory: {base_path}")

    def _block_path(self, block_id: int) -> str:
        return os.path.join(self.base_path, f"block_{block_id:04d}.bin")

    def measure_write(self, block_id: int, extra_delay_ms: float = 0.0) -> float:
        path = self._block_path(block_id)
        if extra_delay_ms > 0:
            time.sleep(extra_delay_ms / 1000.0)
        t0 = time.perf_counter()
        with open(path, "wb") as f:
            f.write(self._buf)
            f.flush()
            os.fsync(f.fileno())
        return (time.perf_counter() - t0) * 1000.0

    def measure_read(self, block_id: int) -> float:
        path = self._block_path(block_id)
        if not os.path.exists(path):
            self.measure_write(block_id)
        t0 = time.perf_counter()
        with open(path, "rb") as f:
            _ = f.read()
        return (time.perf_counter() - t0) * 1000.0

    def cleanup(self):
        import shutil
        shutil.rmtree(self.base_path, ignore_errors=True)


# ─────────────────────────────────────────────────────
# STRESS ENGINE
# ─────────────────────────────────────────────────────
class StressEngine:
    MODES = ["random", "mixed", "sequential"]

    def __init__(self, telemetry: Telemetry, blocks: Dict[int, BlockState]):
        self.telemetry = telemetry
        self.blocks = blocks
        self.mode = "mixed"
        self.intensity = 0.5
        self._degraded: Dict[int, float] = {}

    def inject_degradation(self, block_id: int, delay_ms: float):
        self._degraded[block_id] = delay_ms
        self.blocks[block_id].degradation_injected = True
        log.info(f"[INJECT] Block {block_id} +{delay_ms:.0f} ms delay")

    def run_tick(self) -> List[Tuple[int, str, float]]:
        results = []
        count = max(1, int(self.intensity * 5))
        available = [b for b in self.blocks if not self.blocks[b].is_avoided] or list(self.blocks)
        targets = random.sample(available, min(count, len(available)))

        for blk_id in targets:
            extra = self._degraded.get(blk_id, 0.0)
            if random.random() < 0.04:
                extra += random.uniform(20, 60)

            op = "write" if random.random() < 0.6 else "read"
            if self.mode == "sequential":
                op = "write"
            elif self.mode == "random":
                op = random.choice(["read", "write"])

            if op == "write":
                lat = self.telemetry.measure_write(blk_id, extra)
                self.blocks[blk_id].write_count += 1
            else:
                lat = self.telemetry.measure_read(blk_id)
                if extra > 0:
                    lat += extra
                self.blocks[blk_id].read_count += 1

            self.blocks[blk_id].access_count += 1
            self.blocks[blk_id].latency_history.append(lat)
            results.append((blk_id, op, lat))

        return results


# ─────────────────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────────────────
class DecisionEngine:
    def __init__(self, blocks: Dict[int, BlockState]):
        self.blocks = blocks
        self.avoided_blocks: List[int] = []
        self.action_log: deque = deque(maxlen=200)

    def _log(self, msg: str):
        self.action_log.appendleft({"ts": time.time(), "msg": msg})
        log.info(f"[DECISION] {msg}")

    def evaluate(self):
        for blk_id, blk in self.blocks.items():
            cls = blk.classification
            if cls == "CRITICAL" and not blk.is_avoided:
                blk.is_avoided = True
                self.avoided_blocks.append(blk_id)
                self._log(f"🔴 Block {blk_id} CRITICAL (risk={blk.risk_score:.2f}) — writes redirected")
            elif blk.is_avoided and not blk.degradation_injected:
                if blk.risk_score < ELEVATED_THRESHOLD * 0.8:
                    blk.is_avoided = False
                    if blk_id in self.avoided_blocks:
                        self.avoided_blocks.remove(blk_id)
                    self._log(f"✅ Block {blk_id} recovered — re-enabled (risk={blk.risk_score:.2f})")
            elif cls == "ELEVATED":
                self._log(f"🟡 Block {blk_id} elevated (risk={blk.risk_score:.2f})")

    def top_risky(self, n: int = 5) -> List[dict]:
        # Exclude avoided blocks — they're already handled; show only active risks
        active = [b for b in self.blocks.values() if not b.is_avoided]
        ranked = sorted(active, key=lambda b: b.risk_score, reverse=True)
        return [
            {
                "block_id": b.block_id,
                "risk_score": b.risk_score,
                "classification": b.classification,
                "avg_latency": round(b.avg_latency, 2),
                "variance": round(b.latency_variance, 2),
            }
            for b in ranked[:n]
        ]


# ─────────────────────────────────────────────────────
# MAIN SYSTEM
# ─────────────────────────────────────────────────────
class StorageGuardSystem:
    def __init__(self, base_path: str):
        self.blocks: Dict[int, BlockState] = {i: BlockState(block_id=i) for i in range(NUM_BLOCKS)}
        self.telemetry  = Telemetry(base_path)
        self.stress     = StressEngine(self.telemetry, self.blocks)
        self.decision   = DecisionEngine(self.blocks)
        self.base_path  = base_path

        self._tick           = 0
        self._total_writes   = 0
        self._total_reads    = 0
        self._avoided_writes = 0
        self._trad_latency   = 0.0
        self._prop_latency   = 0.0

        # Scheduled controlled fault injections
        self._inject_schedule = {10: (4, 35.0), 22: (11, 50.0), 38: (19, 70.0)}

    def tick(self) -> dict:
        self._tick += 1
        if self._tick in self._inject_schedule:
            blk, delay = self._inject_schedule[self._tick]
            self.stress.inject_degradation(blk, delay)

        results = self.stress.run_tick()
        self.decision.evaluate()

        for blk_id, op, lat in results:
            if op == "write":
                self._total_writes += 1
                if self.blocks[blk_id].is_avoided:
                    self._avoided_writes += 1
            else:
                self._total_reads += 1
            self._trad_latency += lat
            self._prop_latency += (5.0 if self.blocks[blk_id].is_avoided else lat)

        return self._build_payload()

    def _build_payload(self) -> dict:
        blocks_data = [
            {
                "id": b.block_id,
                "risk": b.risk_score,
                "cls": b.classification,
                "avg_lat": round(b.avg_latency, 2),
                "variance": round(b.latency_variance, 2),
                "trend": round(b.recent_trend, 4),
                "access": b.access_count,
                "avoided": b.is_avoided,
                "injected": b.degradation_injected,
            }
            for b in self.blocks.values()
        ]

        all_readings = []
        for b in self.blocks.values():
            all_readings.extend(list(b.latency_history)[-5:])
        lat_series = [round(x, 2) for x in all_readings[-60:]]

        action_log = [{"ts": e["ts"], "msg": e["msg"]} for e in list(self.decision.action_log)[:20]]

        ops = self._total_writes + self._total_reads
        return {
            "tick": self._tick,
            "ts": time.time(),
            "blocks": blocks_data,
            "top_risky": self.decision.top_risky(5),
            "latency_series": lat_series,
            "action_log": action_log,
            "sd_path": self.base_path,
            "stats": {
                "total_writes": self._total_writes,
                "total_reads": self._total_reads,
                "avoided_writes": self._avoided_writes,
                "avoided_blocks": len(self.decision.avoided_blocks),
                "trad_avg_lat": round(self._trad_latency / max(ops, 1), 2),
                "prop_avg_lat": round(self._prop_latency / max(ops, 1), 2),
            },
            "mode": self.stress.mode,
            "intensity": self.stress.intensity,
        }

    def set_mode(self, mode: str):
        if mode in StressEngine.MODES:
            self.stress.mode = mode

    def set_intensity(self, intensity: float):
        self.stress.intensity = max(0.1, min(1.0, intensity))

    def inject(self, block_id: int, delay_ms: float):
        if block_id in self.blocks:
            self.stress.inject_degradation(block_id, delay_ms)

    def cleanup(self):
        self.telemetry.cleanup()


# ─────────────────────────────────────────────────────
# WEBSOCKET SERVER
# ─────────────────────────────────────────────────────
async def ws_handler(websocket, system: StorageGuardSystem):
    addr = getattr(websocket, "remote_address", "unknown")
    log.info(f"Client connected: {addr}")
    try:
        async def send_loop():
            while True:
                payload = await asyncio.get_event_loop().run_in_executor(None, system.tick)
                await websocket.send(json.dumps(payload))
                await asyncio.sleep(TICK_INTERVAL_S)

        async def recv_loop():
            async for msg in websocket:
                try:
                    cmd = json.loads(msg)
                    t = cmd.get("type")
                    if t == "set_mode":
                        system.set_mode(cmd["value"])
                    elif t == "set_intensity":
                        system.set_intensity(float(cmd["value"]))
                    elif t == "inject":
                        system.inject(int(cmd["block"]), float(cmd["delay"]))
                except Exception as e:
                    log.warning(f"Bad command: {e}")

        await asyncio.gather(send_loop(), recv_loop())
    except Exception as e:
        log.info(f"Client disconnected: {e}")


async def main(path: str, port: int):
    # Try to import websockets; auto-install if missing
    try:
        import websockets
    except ImportError:
        log.info("Installing websockets library…")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
        import websockets

    system = StorageGuardSystem(path)
    log.info(f"SD card path : {path}")
    log.info(f"WebSocket    : ws://localhost:{port}")
    log.info(f"Dashboard    : open storageguard_dashboard.html in your browser")
    log.info("-" * 55)

    try:
        async with websockets.serve(lambda ws: ws_handler(ws, system), "0.0.0.0", port):
            await asyncio.Future()
    finally:
        system.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="StorageGuard Backend")
    parser.add_argument("--path", default=None,
                        help="SD card drive or mount point (e.g. E:\\ or /media/sd)")
    parser.add_argument("--port", type=int, default=8765, help="WebSocket port (default: 8765)")
    args = parser.parse_args()

    resolved_path = choose_path(args.path)
    asyncio.run(main(resolved_path, args.port))
