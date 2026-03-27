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
import heapq
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
BLOCK_SIZE_BYTES   = 256 * 1024     # 256 KB per logical block
HISTORY_LEN        = 60
TICK_INTERVAL_S    = 0.6

STATE_HEALTHY = "HEALTHY"
STATE_WEAK = "WEAK"
STATE_CRITICAL = "CRITICAL"
STATE_RETIRED = "RETIRED"

BUCKET_ORDER = [STATE_HEALTHY, STATE_WEAK, STATE_CRITICAL, STATE_RETIRED]
SELECTABLE_BUCKETS = [STATE_HEALTHY, STATE_WEAK, STATE_CRITICAL]
READ_PREFERRED_BUCKETS = [STATE_HEALTHY, STATE_WEAK]
READ_ERROR_SCALE = 10
WRITE_ERROR_SCALE = 10
RETENTION_ERROR_SECONDS = 12.0
READ_DISTURB_SCALE = 14
MAX_IDLE_SECONDS = 60.0
MAX_READ_LIMIT = 100
WEAR_WEIGHT = 0.5
RETENTION_WEIGHT = 0.3
READ_DISTURB_WEIGHT = 0.2
ERROR_NOISE_MAX = 0.18
ERROR_OUTPUT_SCALE = 4.0
BASE_WRITE_LATENCY_MS = 8.0
BASE_READ_LATENCY_MS = 6.5
LATENCY_NORMALIZATION_MS = 30.0
WRITE_LATENCY_SWING_MS = 4.5
READ_LATENCY_SWING_MS = 4.0
WRITE_NOISE_MS = 0.8
READ_NOISE_MS = 0.6
MAX_LATENCY_SCORE_MS = 25.0
ECC_STAGES = [
    {"name": "LIGHT", "capacity": 2, "latency_cost": 1},
    {"name": "MEDIUM", "capacity": 5, "latency_cost": 2},
    {"name": "STRONG", "capacity": 10, "latency_cost": 3},
]
ECC_CORRECTION_LIMIT = ECC_STAGES[0]["capacity"]
INTELLIGENCE_WRITE_INTERVAL = 5

# -- Intelligence Tuning --
MAX_PE                = 100
MAX_WRITE_LIMIT       = MAX_PE
BASELINE_LATENCY      = 10.0
MAX_RETENTION         = 60.0
ECC_GROWTH_LIMIT      = 0.15
LATENCY_GROWTH_LIMIT  = 0.10
BASE_ECC_CRITICAL     = 6.0
BASE_LATENCY_LIMIT    = 18.0
BASE_RETRY_LIMIT      = 5.0
HEALTH_WARNING        = 0.60
HEALTH_CRITICAL       = 0.80
ELEVATED_THRESHOLD    = HEALTH_WARNING
CRITICAL_THRESHOLD    = HEALTH_CRITICAL
HISTORY_WINDOW        = 10
HYSTERESIS_MARGIN     = 0.05
EVAL_INTERVAL_TICKS   = 3
MIGRATION_BATCH       = 2


# ---------------------------------------------------------
# INTELLIGENCE FUNCTIONS (Feature Engineering + Score + Thresholds + Trend)
# ---------------------------------------------------------
def extract_features(block) -> dict:
    """Normalize raw block metrics into comparable 0-1 signals."""
    now = time.time()
    retention_age = max(0.0, now - block.last_write_at)

    # ECC growth rate: change in ecc errors per second
    time_delta = max(1.0, now - block.prev_eval_time)
    ecc_growth = max(0.0, (block.total_generated_errors - block.prev_ecc_errors) / time_delta)

    # Latency growth: compare recent vs older latency
    h = list(block.latency_history)
    if len(h) >= 4:
        half = len(h) // 2
        older_avg = sum(h[:half]) / half
        newer_avg = sum(h[half:]) / (len(h) - half)
        latency_growth = max(0.0, (newer_avg - older_avg) / max(older_avg, 1e-6))
    else:
        latency_growth = 0.0

    return {
        "pe_norm": min(1.0, block.write_count / max(MAX_PE, 1)),
        "ecc_rate": min(1.0, block.total_generated_errors / max(block.read_count * ECC_STAGES[-1]["capacity"], 1)),
        "latency_norm": min(1.0, block.avg_latency / max(BASELINE_LATENCY * 3.0, 1.0)),
        "retry_rate": min(1.0, block.failed_read_count / max(block.read_count, 1)),
        "retention_norm": min(1.0, retention_age / max(MAX_RETENTION, 1.0)),
        "ecc_growth": min(1.0, ecc_growth / max(ECC_GROWTH_LIMIT * 2.0, 1e-6)),
        "latency_growth": min(1.0, latency_growth),
    }


def compute_health_score(features: dict) -> float:
    """Weighted composite health score. Returns 0-1 where higher = worse."""
    return (
        0.25 * features["pe_norm"] +
        0.20 * features["ecc_rate"] +
        0.15 * features["latency_norm"] +
        0.10 * features["retry_rate"] +
        0.15 * features["retention_norm"] +
        0.15 * features["ecc_growth"]
    )


def dynamic_thresholds(block) -> dict:
    """Thresholds that adapt as NAND ages -- not fixed constants."""
    wear_factor = min(1.0, block.write_count / max(MAX_PE, 1))
    return {
        "ecc_critical": BASE_ECC_CRITICAL * (1.0 + wear_factor),
        "latency_limit": BASE_LATENCY_LIMIT * (1.0 + wear_factor),
        "retry_limit": BASE_RETRY_LIMIT * (1.0 + wear_factor * 0.5),
    }


def trend_analysis(block) -> str:
    """Check growth rates for early warning. Returns STABLE or WARNING."""
    now = time.time()
    time_delta = max(1.0, now - block.prev_eval_time)
    ecc_growth = max(0.0, (block.total_generated_errors - block.prev_ecc_errors) / time_delta)

    h = list(block.latency_history)
    if len(h) >= 4:
        half = len(h) // 2
        older_avg = sum(h[:half]) / half
        newer_avg = sum(h[half:]) / (len(h) - half)
        latency_growth = max(0.0, (newer_avg - older_avg) / max(older_avg, 1e-6))
    else:
        latency_growth = 0.0

    if ecc_growth > ECC_GROWTH_LIMIT:
        return "WARNING"
    if latency_growth > LATENCY_GROWTH_LIMIT:
        return "WARNING"
    return "STABLE"


def evaluate_block(block) -> str:
    """Full decision engine: hard-fail + trend + score with hysteresis."""
    features = extract_features(block)
    score = compute_health_score(features)
    th = dynamic_thresholds(block)

    # 1. HARD FAIL (immediate)
    if block.write_count > MAX_PE:
        return "CRITICAL"
    if block.total_generated_errors > th["ecc_critical"] * max(block.read_count, 1):
        return "CRITICAL"

    # 2. TREND-BASED WARNING
    trend = trend_analysis(block)
    if trend == "WARNING":
        # Hysteresis: if already NORMAL, need stronger signal
        if block.intelligence_state == "NORMAL" and score > HEALTH_WARNING - HYSTERESIS_MARGIN:
            return "WARNING"
        elif block.intelligence_state != "NORMAL":
            return "WARNING"

    # 3. SCORE-BASED with hysteresis
    if score > HEALTH_CRITICAL:
        return "CRITICAL"
    elif score > HEALTH_WARNING:
        return "WARNING"
    else:
        # Hysteresis: don't drop from WARNING to NORMAL too easily
        if block.intelligence_state == "WARNING" and score > HEALTH_WARNING - HYSTERESIS_MARGIN:
            return "WARNING"
        return "NORMAL"


# ---------------------------------------------------------
# SD CARD AUTO-DETECTION  (Windows + Linux/macOS)
# ---------------------------------------------------------
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
    write_count: int = 0
    error_count: float = 0.0
    latency: float = 1.0
    state: str = STATE_HEALTHY
    last_operation: str = "IDLE"
    last_error_generated: int = 0
    last_ecc_status: str = "N/A"
    last_intelligence_triggered: bool = False
    latency_history: deque = field(default_factory=lambda: deque(maxlen=HISTORY_LEN))
    access_count: int = 0
    read_count: int = 0
    is_avoided: bool = False
    degradation_injected: bool = False
    created_at: float = field(default_factory=time.time)
    last_write_at: float = field(default_factory=time.time)
    last_read_at: float = field(default_factory=time.time)
    physical_write_count: float = 0.0
    total_generated_errors: float = 0.0
    corrected_read_count: int = 0
    failed_read_count: int = 0
    # Intelligence tracking fields
    prev_ecc_errors: float = 0.0
    prev_eval_time: float = field(default_factory=time.time)
    intelligence_state: str = "NORMAL"
    read_retries: int = 0

    def to_metadata(self) -> dict:
        return {
            "block_id": self.block_id,
            "write_count": self.write_count,
            "error_count": self.error_count,
            "latency": self.latency,
            "state": self.state,
            "last_operation": self.last_operation,
            "last_error_generated": self.last_error_generated,
            "last_ecc_status": self.last_ecc_status,
            "last_intelligence_triggered": self.last_intelligence_triggered,
            "latency_history": list(self.latency_history),
            "access_count": self.access_count,
            "read_count": self.read_count,
            "is_avoided": self.is_avoided,
            "degradation_injected": self.degradation_injected,
            "created_at": self.created_at,
            "last_write_at": self.last_write_at,
            "last_read_at": self.last_read_at,
            "physical_write_count": self.physical_write_count,
            "total_generated_errors": self.total_generated_errors,
            "corrected_read_count": self.corrected_read_count,
            "failed_read_count": self.failed_read_count,
            "prev_ecc_errors": self.prev_ecc_errors,
            "prev_eval_time": self.prev_eval_time,
            "intelligence_state": self.intelligence_state,
            "read_retries": self.read_retries,
        }

    def apply_metadata(self, payload: dict):
        if payload.get("block_id") != self.block_id:
            return
        self.write_count = int(payload.get("write_count", 0))
        self.error_count = float(payload.get("error_count", 0.0))
        self.latency = float(payload.get("latency", 1.0))
        self.state = str(payload.get("state", STATE_HEALTHY))
        self.last_operation = str(payload.get("last_operation", "IDLE"))
        self.last_error_generated = int(payload.get("last_error_generated", 0))
        self.last_ecc_status = str(payload.get("last_ecc_status", "N/A"))
        self.last_intelligence_triggered = bool(payload.get("last_intelligence_triggered", False))
        history = payload.get("latency_history", [])
        self.latency_history = deque((float(x) for x in history), maxlen=HISTORY_LEN)
        self.access_count = int(payload.get("access_count", 0))
        self.read_count = int(payload.get("read_count", 0))
        self.is_avoided = bool(payload.get("is_avoided", False))
        self.degradation_injected = bool(payload.get("degradation_injected", False))
        now = time.time()
        self.created_at = float(payload.get("created_at", now))
        self.last_write_at = float(payload.get("last_write_at", self.created_at))
        self.last_read_at = float(payload.get("last_read_at", self.created_at))
        self.physical_write_count = float(payload.get("physical_write_count", float(self.write_count)))
        self.total_generated_errors = float(payload.get("total_generated_errors", 0.0))
        self.corrected_read_count = int(payload.get("corrected_read_count", 0))
        self.failed_read_count = int(payload.get("failed_read_count", 0))
        self.prev_ecc_errors = float(payload.get("prev_ecc_errors", 0.0))
        self.prev_eval_time = float(payload.get("prev_eval_time", now))
        self.intelligence_state = str(payload.get("intelligence_state", "NORMAL"))
        self.read_retries = int(payload.get("read_retries", 0))

    def record_write(self, latency_ms: float):
        self.write_count += 1
        self.access_count += 1
        self.last_operation = "WRITE"
        self.latency = latency_ms
        self.latency_history.append(latency_ms)
        self.last_write_at = time.time()

    def record_read(self, latency_ms: float):
        self.read_count += 1
        self.access_count += 1
        self.last_operation = "READ"
        self.latency = latency_ms
        self.latency_history.append(latency_ms)
        self.last_read_at = time.time()

    def record_error(self, count: float = 1.0):
        self.error_count += max(0.0, float(count))

    def record_generated_errors(self, count: float):
        self.total_generated_errors += max(0.0, float(count))

    def record_physical_writes(self, count: float):
        self.physical_write_count += max(0.0, float(count))

    def record_corrected_read(self):
        self.corrected_read_count += 1

    def record_failed_read(self):
        self.failed_read_count += 1

    def set_state(self, state: str):
        self.state = state

    def update_latency(self, latency_ms: float):
        self.latency = latency_ms
        self.latency_history.append(latency_ms)

    def reset_runtime(self):
        self.write_count = 0
        self.error_count = 0.0
        self.latency = 1.0
        self.state = STATE_HEALTHY
        self.last_operation = "IDLE"
        self.last_error_generated = 0
        self.last_ecc_status = "N/A"
        self.last_intelligence_triggered = False
        self.latency_history.clear()
        self.access_count = 0
        self.read_count = 0
        self.is_avoided = False
        self.degradation_injected = False
        self.created_at = time.time()
        self.last_write_at = self.created_at
        self.last_read_at = self.created_at
        self.physical_write_count = 0.0
        self.total_generated_errors = 0.0
        self.corrected_read_count = 0
        self.failed_read_count = 0
        self.prev_ecc_errors = 0.0
        self.prev_eval_time = self.created_at
        self.intelligence_state = "NORMAL"
        self.read_retries = 0

    def sync_state(self):
        if self.is_avoided:
            self.state = STATE_RETIRED
            return
        decision = evaluate_block(self)
        self.intelligence_state = decision
        now = time.time()
        self.prev_ecc_errors = self.total_generated_errors
        self.prev_eval_time = now
        if decision == "CRITICAL":
            self.state = STATE_CRITICAL
        elif decision == "WARNING":
            self.state = STATE_WEAK
        else:
            self.state = STATE_HEALTHY

    @property
    def avg_latency(self) -> float:
        if self.latency_history:
            return sum(self.latency_history) / len(self.latency_history)
        return self.latency

    @property
    def wear_component(self) -> float:
        return round(min(1.0, self.write_count / max(MAX_WRITE_LIMIT, 1)), 4)

    @property
    def error_rate(self) -> float:
        denom = max(self.read_count * ECC_STAGES[-1]["capacity"], 1)
        return round(min(1.0, self.total_generated_errors / denom), 4)

    @property
    def ecc_correction_rate(self) -> float:
        return round(self.corrected_read_count / max(self.read_count, 1), 4)

    @property
    def write_amplification_factor(self) -> float:
        return round(self.physical_write_count / max(self.write_count, 1), 4) if self.write_count else 1.0

    @property
    def health_score(self) -> float:
        """0-100 scale health score using the intelligence pipeline."""
        features = extract_features(self)
        raw = compute_health_score(features)
        return round(max(0.0, min(100.0, 100.0 * (1.0 - raw))), 2)

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
        """0-1 scale risk using the intelligence pipeline."""
        features = extract_features(self)
        return round(min(max(compute_health_score(features), 0.0), 1.0), 4)

    @property
    def classification(self) -> str:
        if self.is_avoided:
            return "AVOIDED"
        return self.intelligence_state


class BucketManager:
    def __init__(self, blocks: List[BlockState], storage_path: Optional[str] = None):
        self.buckets: Dict[str, List[int]] = {name: [] for name in BUCKET_ORDER}
        self.pointer: Dict[str, int] = {name: 0 for name in SELECTABLE_BUCKETS}
        self.block_bucket: Dict[int, str] = {}
        self.state_path = os.path.join(storage_path, "bucket_state.json") if storage_path else None
        self.sync_all(blocks)

    def _persist_state(self):
        if not self.state_path:
            return
        payload = {
            "updated_at": time.time(),
            "buckets": self.snapshot(),
            "pointers": dict(self.pointer),
            "block_bucket": dict(self.block_bucket),
        }
        tmp_path = f"{self.state_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        os.replace(tmp_path, self.state_path)

    def _remove_from_bucket(self, bucket_name: str, block_id: int):
        bucket = self.buckets[bucket_name]
        try:
            idx = bucket.index(block_id)
        except ValueError:
            return

        bucket.pop(idx)
        if bucket_name in self.pointer:
            pointer = self.pointer[bucket_name]
            if idx < pointer:
                pointer -= 1
            self.pointer[bucket_name] = pointer % len(bucket) if bucket else 0

    def _add_to_bucket(self, bucket_name: str, block_id: int):
        bucket = self.buckets[bucket_name]
        if block_id not in bucket:
            bucket.append(block_id)

    def sync_block(self, block: BlockState):
        desired = block.state
        current = self.block_bucket.get(block.block_id)
        if current == desired:
            return

        if current is not None:
            self._remove_from_bucket(current, block.block_id)

        self._add_to_bucket(desired, block.block_id)
        self.block_bucket[block.block_id] = desired
        self._persist_state()

    def sync_all(self, blocks: List[BlockState]):
        for block in blocks:
            block.sync_state()
            self.sync_block(block)
        self._persist_state()

    def select_next(self) -> Optional[int]:
        selection = self.select_next_detail()
        return selection[0] if selection else None

    def select_next_detail(self, bucket_order: Optional[List[str]] = None) -> Optional[Tuple[int, str, int]]:
        for bucket_name in (bucket_order or SELECTABLE_BUCKETS):
            bucket = self.buckets[bucket_name]
            if not bucket:
                continue
            pointer = self.pointer[bucket_name] % len(bucket)
            block_id = bucket[pointer]
            self.pointer[bucket_name] = (pointer + 1) % len(bucket)
            self._persist_state()
            return block_id, bucket_name, pointer
        self._persist_state()
        return None

    def select_specific_block(self, block_id: int) -> Optional[Tuple[int, str, int]]:
        bucket_name = self.block_bucket.get(block_id)
        if bucket_name is None or bucket_name not in SELECTABLE_BUCKETS:
            return None
        bucket = self.buckets.get(bucket_name, [])
        if block_id not in bucket:
            return None
        pointer = bucket.index(block_id)
        self.pointer[bucket_name] = (pointer + 1) % len(bucket)
        self._persist_state()
        return block_id, bucket_name, pointer

    def snapshot(self) -> Dict[str, List[int]]:
        return {name: list(blocks) for name, blocks in self.buckets.items()}

    def pointer_target(self, bucket_name: str) -> Optional[int]:
        bucket = self.buckets.get(bucket_name, [])
        if not bucket or bucket_name not in self.pointer:
            return None
        pointer = self.pointer[bucket_name] % len(bucket)
        return bucket[pointer]

    def reset(self, blocks: List[BlockState]):
        self.buckets = {name: [] for name in BUCKET_ORDER}
        self.pointer = {name: 0 for name in SELECTABLE_BUCKETS}
        self.block_bucket = {}
        self.sync_all(blocks)
        self._persist_state()


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

    def measure_write_data(self, block_id: int, data: bytes, extra_delay_ms: float = 0.0) -> float:
        path = self._block_path(block_id)
        if extra_delay_ms > 0:
            time.sleep(extra_delay_ms / 1000.0)
        t0 = time.perf_counter()
        with open(path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        return (time.perf_counter() - t0) * 1000.0

    def measure_read_data(self, block_id: int) -> Tuple[float, bytes]:
        path = self._block_path(block_id)
        if not os.path.exists(path):
            self.measure_write(block_id)
        t0 = time.perf_counter()
        with open(path, "rb") as f:
            data = f.read()
        return (time.perf_counter() - t0) * 1000.0, data

    def cleanup(self):
        import shutil
        shutil.rmtree(self.base_path, ignore_errors=True)

    def reset(self):
        self.cleanup()
        os.makedirs(self.base_path, exist_ok=True)
        self._buf = os.urandom(BLOCK_SIZE_BYTES)


# ─────────────────────────────────────────────────────
# STRESS ENGINE
# ─────────────────────────────────────────────────────
class StressEngine:
    MODES = ["random", "mixed", "sequential"]

    def __init__(self, telemetry: Telemetry, blocks: List[BlockState], bucket_manager: BucketManager):
        self.telemetry = telemetry
        self.blocks = blocks
        self.bucket_manager = bucket_manager
        self.mode = "mixed"
        self.intensity = 1.0
        self._degraded: Dict[int, float] = {}

    def inject_degradation(self, block_id: int, delay_ms: float):
        self._degraded[block_id] = delay_ms
        self.blocks[block_id].degradation_injected = True
        log.info(f"[INJECT] Block {block_id} +{delay_ms:.0f} ms delay")

    def reset(self):
        self._degraded.clear()

    def _error_model(self, block: BlockState) -> int:
        return self._composite_error_sources(block, include_read_disturb=True)["total"]

    def _composite_error_sources(self, block: BlockState, include_read_disturb: bool) -> dict:
        now = time.time()
        wear_component = min(1.0, block.wear_component)
        retention_age = max(0.0, now - block.last_write_at)
        retention_component = min(1.0, retention_age / max(MAX_IDLE_SECONDS, 1.0))
        read_disturb_component = min(1.0, (block.read_count / max(MAX_READ_LIMIT, 1))) if include_read_disturb else 0.0
        error_rate_component = min(1.0, block.error_rate * 4.0)
        latency_component = min(1.0, block.avg_latency / MAX_LATENCY_SCORE_MS)
        health_penalty = min(1.0, max(0.0, (100.0 - block.health_score) / 100.0))
        waf_component = min(1.0, max(0.0, block.write_amplification_factor - 1.0) / 1.5)
        random_noise = random.uniform(0.0, ERROR_NOISE_MAX)
        error_score = (
            (0.25 * wear_component) +
            (0.15 * retention_component) +
            (0.10 * read_disturb_component) +
            (0.18 * error_rate_component) +
            (0.12 * latency_component) +
            (0.12 * health_penalty) +
            (0.08 * waf_component) +
            random_noise
        )
        total = max(0, int(error_score * ERROR_OUTPUT_SCALE))
        return {
            "wear_component": round(wear_component, 2),
            "retention_component": round(retention_component, 2),
            "retention_age_s": round(retention_age, 2),
            "read_disturb_component": round(read_disturb_component, 2),
            "error_rate_component": round(error_rate_component, 2),
            "latency_component": round(latency_component, 2),
            "health_penalty": round(health_penalty, 2),
            "waf_component": round(waf_component, 2),
            "random_noise": round(random_noise, 2),
            "error_score": round(error_score, 2),
            "total": total,
        }

    def _write_latency(self, block: BlockState, measured_ms: float, extra_delay_ms: float) -> float:
        wear = min(1.6, block.write_count * 0.03)
        normalized_current = min(1.0, measured_ms / LATENCY_NORMALIZATION_MS) * WRITE_LATENCY_SWING_MS
        random_noise = random.uniform(0.0, WRITE_NOISE_MS)
        waf_penalty = min(2.0, max(0.0, block.write_amplification_factor - 1.0) * 1.4)
        health_penalty = min(1.4, ((100.0 - block.health_score) / 100.0) * 1.4)
        return BASE_WRITE_LATENCY_MS + normalized_current + random_noise + extra_delay_ms + wear + waf_penalty + health_penalty

    def _read_latency(self, measured_ms: float, extra_delay_ms: float, ecc_latency_cost: int) -> float:
        normalized_current = min(1.0, measured_ms / LATENCY_NORMALIZATION_MS) * READ_LATENCY_SWING_MS
        random_noise = random.uniform(0.0, READ_NOISE_MS)
        return BASE_READ_LATENCY_MS + normalized_current + random_noise + extra_delay_ms + float(ecc_latency_cost)

    def _write_amplification_increment(self, block: BlockState) -> float:
        wear = block.wear_component
        error_pressure = min(1.0, block.error_rate * 4.0)
        health_penalty = min(1.0, max(0.0, (100.0 - block.health_score) / 100.0))
        return 1.0 + (0.22 * wear) + (0.14 * error_pressure) + (0.10 * health_penalty)

    def _starting_ecc_stage_index(self, block: BlockState) -> int:
        risk = min(1.0, block.write_count / max(MAX_WRITE_LIMIT, 1))
        if risk < 0.3:
            return 0
        if risk < 0.7:
            return 1
        return 2

    def _resolve_read_ecc(self, block: BlockState, error_generated: int) -> dict:
        start_index = self._starting_ecc_stage_index(block)
        risk = min(1.0, block.write_count / max(MAX_WRITE_LIMIT, 1))
        stage_attempts: List[str] = []
        total_latency_cost = 0

        if error_generated <= 0:
            return {
                "status": "SUCCESS",
                "error_detected": False,
                "risk": risk,
                "start_stage": ECC_STAGES[start_index]["name"],
                "stage_used": "NONE",
                "stage_attempts": stage_attempts,
                "ecc_latency_cost": 0,
                "verification_passed": False,
            }

        for idx in range(start_index, len(ECC_STAGES)):
            stage = ECC_STAGES[idx]
            stage_attempts.append(stage["name"])
            total_latency_cost += stage["latency_cost"]
            if error_generated <= stage["capacity"]:
                return {
                    "status": "CORRECTED",
                    "error_detected": True,
                    "risk": risk,
                    "start_stage": ECC_STAGES[start_index]["name"],
                    "stage_used": stage["name"],
                    "stage_attempts": stage_attempts,
                    "ecc_latency_cost": total_latency_cost,
                    "verification_passed": True,
                }

        return {
            "status": "FAILURE",
            "error_detected": True,
            "risk": risk,
            "start_stage": ECC_STAGES[start_index]["name"],
            "stage_used": ECC_STAGES[-1]["name"],
            "stage_attempts": stage_attempts,
            "ecc_latency_cost": total_latency_cost,
            "verification_passed": False,
        }

    def _read_error_delta(self, status: str, error_generated: int, ecc_result: dict) -> float:
        if status == "SUCCESS":
            return 0.05 * error_generated
        if status == "CORRECTED":
            return 1.0 + (0.2 * error_generated)
        residual_errors = max(0, error_generated - ECC_STAGES[-1]["capacity"])
        return 3.0 + error_generated + residual_errors

    def _select_op(self) -> str:
        if self.mode == "sequential":
            return "write"
        if self.mode == "random":
            return random.choice(["read", "write"])
        return "write" if random.random() < 0.6 else "read"

    def _write_op(self, block: BlockState, bucket_name: str, pointer: int, extra_delay_ms: float) -> dict:
        before_write = block.write_count
        before_error = block.error_count
        before_waf = block.write_amplification_factor
        before_health = block.health_score
        measured = self.telemetry.measure_write(block.block_id, extra_delay_ms)
        latency = self._write_latency(block, measured, extra_delay_ms)
        block.record_write(latency)
        physical_writes = self._write_amplification_increment(block)
        block.record_physical_writes(physical_writes)
        block.update_latency(latency)
        block.last_error_generated = 0
        block.last_ecc_status = "N/A"
        block.last_intelligence_triggered = False
        old_bucket_name = bucket_name
        self.bucket_manager.decision.evaluate_single(block)
        self.bucket_manager.sync_block(block)
        pointer_after = self.bucket_manager.pointer.get(old_bucket_name, 0)

        # Intelligence always triggers on write in event-driven model
        intelligence_triggered = True
        block.last_intelligence_triggered = True

        steps = [
            {
                "title": "Bucket Check",
                "detail": f"Selected {bucket_name}; round-robin pointer {pointer}",
            },
            {
                "title": "Round Robin",
                "detail": f"Block #{block.block_id} chosen from {bucket_name}; pointer advances to {pointer_after}",
            },
            {
                "title": "Write",
                "detail": f"Simulated write completed; latency {latency:.2f} ms",
            },
            {
                "title": "Wear Update",
                "detail": f"write_count {before_write} -> {block.write_count}; wear accumulates only on write operations",
            },
            {
                "title": "Write Amplification",
                "detail": f"physical writes +{physical_writes:.2f}; WAF {before_waf:.2f} -> {block.write_amplification_factor:.2f}; health {before_health:.2f} -> {block.health_score:.2f}",
            },
            {
                "title": "Metadata Update",
                "detail": f"write_count {before_write} -> {block.write_count}; error_count {before_error:.2f} -> {block.error_count:.2f}",
            },
        ]

        if intelligence_triggered:
            steps.append({
                "title": "Intelligence Evaluation",
                "detail": "Block evaluated by event-driven intelligence layer",
            })

        return {
            "operation": "WRITE",
            "block_id": block.block_id,
            "bucket": bucket_name,
            "pointer": pointer,
            "pointer_after": pointer_after,
            "status": "SUCCESS",
            "error_generated": 0,
            "ecc_limit": ECC_CORRECTION_LIMIT,
            "intelligence_triggered": intelligence_triggered,
            "latency": round(latency, 2),
            "write_before": before_write,
            "write_after": block.write_count,
            "error_before": before_error,
            "error_after": round(block.error_count, 2),
            "waf": block.write_amplification_factor,
            "health_score": block.health_score,
            "error_rate": block.error_rate,
            "state": block.state,
            "steps": steps,
        }

    def _read_op(self, block: BlockState, bucket_name: str, pointer: int, extra_delay_ms: float) -> dict:
        before_write = block.write_count
        before_error = block.error_count
        before_health = block.health_score
        measured, raw_data = self.telemetry.measure_read_data(block.block_id)
        error_sources = self._composite_error_sources(block, include_read_disturb=True)
        error_generated = error_sources["total"]
        ecc_result = self._resolve_read_ecc(block, error_generated)
        status = ecc_result["status"]
        latency = self._read_latency(measured, extra_delay_ms, ecc_result["ecc_latency_cost"])
        block.record_read(latency)
        block.update_latency(latency)
        block.last_error_generated = error_generated
        block.last_ecc_status = ecc_result["stage_used"] if status == "CORRECTED" else status
        block.record_generated_errors(error_generated)

        error_delta = self._read_error_delta(status, error_generated, ecc_result)
        if status == "CORRECTED":
            block.record_error(error_delta)
            block.record_corrected_read()
        elif status == "FAILURE":
            block.record_error(error_delta)
            block.record_failed_read()
        else:
            block.record_error(error_delta)

        intelligence_triggered = error_generated > 0
        block.last_intelligence_triggered = intelligence_triggered
        old_bucket_name = bucket_name
        
        if intelligence_triggered:
            self.bucket_manager.decision.evaluate_single(block)
        
        self.bucket_manager.sync_block(block)
        pointer_after = self.bucket_manager.pointer.get(old_bucket_name, 0)
        text_preview = raw_data.decode("utf-8", errors="replace")[:80]

        steps = [
            {
                "title": "Bucket Check",
                "detail": f"Selected {bucket_name}; round-robin pointer {pointer}",
            },
            {
                "title": "Round Robin",
                "detail": f"Block #{block.block_id} chosen from {bucket_name}; pointer advances to {pointer_after}",
            },
            {
                "title": "Read",
                "detail": f"Simulated read completed; latency {latency:.2f} ms",
            },
            {
                "title": "Error Generation",
                "detail": f"Wear {error_sources['wear_component']} + retention {error_sources['retention_component']} ({error_sources['retention_age_s']} s idle) + disturb {error_sources['read_disturb_component']} + error-rate {error_sources['error_rate_component']} + latency {error_sources['latency_component']} + health {error_sources['health_penalty']} + WAF {error_sources['waf_component']} + noise {error_sources['random_noise']} => score {error_sources['error_score']} => {error_generated} error unit(s)",
            },
            {
                "title": "Error Detection",
                "detail": "Error detected" if ecc_result["error_detected"] else "No error detected; read succeeds without correction",
            },
            {
                "title": "ECC Check",
                "detail": f"Risk {ecc_result['risk']:.2f} starts at {ecc_result['start_stage']}; attempts {', '.join(ecc_result['stage_attempts']) or 'none'}; status {status}",
            },
            {
                "title": "Verification",
                "detail": "Verification passed after correction" if ecc_result["verification_passed"] else ("Verification skipped because no correction was needed" if status == "SUCCESS" else "Verification failed because all ECC stages failed"),
            },
            {
                "title": "Metadata Update",
                "detail": f"write_count {before_write} -> {block.write_count}; error_count {before_error:.2f} -> {block.error_count:.2f}; delta +{error_delta:.2f}; error_rate {block.error_rate:.2f}; health {before_health:.2f} -> {block.health_score:.2f}; latency +{ecc_result['ecc_latency_cost']}",
            },
        ]

        if intelligence_triggered:
            steps.append({
                "title": "Intelligence Evaluation",
                "detail": f"Error generation (>0) triggered event-driven intelligence evaluation",
            })

        return {
            "operation": "READ",
            "block_id": block.block_id,
            "bucket": bucket_name,
            "pointer": pointer,
            "pointer_after": pointer_after,
            "status": status,
            "error_generated": error_generated,
            "error_detected": ecc_result["error_detected"],
            "ecc_limit": ECC_STAGES[-1]["capacity"],
            "ecc_stage_used": ecc_result["stage_used"],
            "ecc_stage_attempts": ecc_result["stage_attempts"],
            "verification_passed": ecc_result["verification_passed"],
            "intelligence_triggered": intelligence_triggered,
            "latency": round(latency, 2),
            "write_before": before_write,
            "write_after": block.write_count,
            "error_before": before_error,
            "error_after": round(block.error_count, 2),
            "waf": block.write_amplification_factor,
            "health_score": block.health_score,
            "error_rate": block.error_rate,
            "state": block.state,
            "text_preview": text_preview,
            "steps": steps,
        }

    def run_tick(self) -> List[dict]:
        events = []
        count = max(1, int(self.intensity * 5))
        for _ in range(count):
            op = self._select_op()
            selection = self.bucket_manager.select_next_detail(READ_PREFERRED_BUCKETS if op == "read" else None)
            if selection is None:
                break

            blk_id, bucket_name, pointer = selection
            block = self.blocks[blk_id]
            extra_delay = self._degraded.get(blk_id, 0.0)
            if random.random() < 0.04:
                extra_delay += random.uniform(20, 60)

            event = None
            if op == "write":
                event = self._write_op(block, bucket_name, pointer, extra_delay)
            else:
                event = self._read_op(block, bucket_name, pointer, extra_delay)

            block.last_operation = event["operation"]
            events.append(event)

        return events

    def run_manual_operation(self, operation: str, data_text: str = "", block_id: Optional[int] = None) -> dict:
        bucket_snapshot_before = self.bucket_manager.snapshot()
        pointer_snapshot_before = dict(self.bucket_manager.pointer)
        operation = operation.upper()
        selection = None

        if operation == "READ" and block_id is not None:
            selection = self.bucket_manager.select_specific_block(block_id)
            if selection is None:
                selected_bucket = self.bucket_manager.block_bucket.get(block_id, "UNKNOWN")
                message = f"Block #{block_id} cannot be read from state {selected_bucket}. Choose a HEALTHY, WEAK, or CRITICAL block."
                return {
                    "operation": operation,
                    "block_id": block_id,
                    "bucket": selected_bucket,
                    "pointer": None,
                    "pointer_after": None,
                    "status": "INVALID_SELECTION",
                    "error_generated": 0,
                    "ecc_limit": ECC_STAGES[-1]["capacity"],
                    "intelligence_triggered": False,
                    "latency": 0.0,
                    "write_before": 0,
                    "write_after": 0,
                    "error_before": 0,
                    "error_after": 0,
                    "state": selected_bucket,
                    "data_size": 0,
                    "text_preview": "",
                    "bucket_snapshot_before": bucket_snapshot_before,
                    "bucket_snapshot_after": bucket_snapshot_before,
                    "pointer_snapshot_before": pointer_snapshot_before,
                    "pointer_snapshot_after": dict(self.bucket_manager.pointer),
                    "bucket_state_path": self.bucket_manager.state_path,
                    "steps": [
                        {
                            "title": "Block Selection",
                            "detail": f"Manual read requested block #{block_id}.",
                        },
                        {
                            "title": "Selection Rejected",
                            "detail": message,
                        },
                    ],
                }
        elif operation == "READ":
            selection = self.bucket_manager.select_next_detail(READ_PREFERRED_BUCKETS)
        else:
            selection = self.bucket_manager.select_next_detail()
        if selection is None:
            message = "Data cannot be stored because HEALTHY, WEAK, and CRITICAL buckets are empty." if operation == "WRITE" else "Data cannot be read because HEALTHY and WEAK buckets are empty."
            return {
                "operation": operation,
                "block_id": None,
                "bucket": "NONE",
                "pointer": None,
                "pointer_after": None,
                "status": "NO_CAPACITY",
                "error_generated": 0,
                "ecc_limit": ECC_STAGES[-1]["capacity"],
                "intelligence_triggered": False,
                "latency": 0.0,
                "write_before": 0,
                "write_after": 0,
                "error_before": 0,
                "error_after": 0,
                "state": "UNAVAILABLE",
                "data_size": 0,
                "text_preview": "",
                "bucket_snapshot_before": bucket_snapshot_before,
                "bucket_snapshot_after": bucket_snapshot_before,
                "pointer_snapshot_before": pointer_snapshot_before,
                "pointer_snapshot_after": dict(self.bucket_manager.pointer),
                "bucket_state_path": self.bucket_manager.state_path,
                "steps": [
                    {
                        "title": "Bucket Lookup",
                        "detail": "Checked the stored bucket metadata in O(1) time complexity.",
                    },
                    {
                        "title": "No Capacity",
                        "detail": message,
                    },
                ],
            }

        blk_id, bucket_name, pointer = selection
        block = self.blocks[blk_id]
        extra_delay = self._degraded.get(blk_id, 0.0)

        if operation == "WRITE":
            data_bytes = data_text.encode("utf-8")
            if len(data_bytes) > BLOCK_SIZE_BYTES:
                raise ValueError(f"Input exceeds block size of {BLOCK_SIZE_BYTES} bytes")

            before_write = block.write_count
            before_error = block.error_count
            before_waf = block.write_amplification_factor
            before_health = block.health_score
            measured = self.telemetry.measure_write_data(block.block_id, data_bytes, extra_delay)
            latency = self._write_latency(block, measured, extra_delay)
            block.record_write(latency)
            physical_writes = self._write_amplification_increment(block)
            block.record_physical_writes(physical_writes)
            block.update_latency(latency)
            block.last_error_generated = 0
            block.last_ecc_status = "N/A"
            block.last_intelligence_triggered = False
            old_bucket_name = bucket_name
            block.sync_state()
            self.bucket_manager.sync_block(block)
            pointer_snapshot_after = dict(self.bucket_manager.pointer)
            bucket_snapshot_after = self.bucket_manager.snapshot()
            pointer_after = pointer_snapshot_after.get(old_bucket_name, 0)
            next_block_id = self.bucket_manager.pointer_target(old_bucket_name)
            state_note = ""
            if block.state != old_bucket_name:
                state_note = f" Block moved from {old_bucket_name} to {block.state}, and the stored pointers were updated."

            return {
                "operation": "WRITE",
                "block_id": block.block_id,
                "bucket": bucket_name,
                "pointer": pointer,
                "pointer_after": pointer_after,
                "status": "WRITTEN",
                "error_generated": 0,
                "ecc_limit": ECC_CORRECTION_LIMIT,
                "intelligence_triggered": False,
                "latency": round(latency, 2),
                "write_before": before_write,
                "write_after": block.write_count,
                "error_before": before_error,
                "error_after": round(block.error_count, 2),
                "waf": block.write_amplification_factor,
                "health_score": block.health_score,
                "error_rate": block.error_rate,
                "state": block.state,
                "data_size": len(data_bytes),
                "text_preview": data_text[:80],
                "bucket_snapshot_before": bucket_snapshot_before,
                "bucket_snapshot_after": bucket_snapshot_after,
                "pointer_snapshot_before": pointer_snapshot_before,
                "pointer_snapshot_after": pointer_snapshot_after,
                "bucket_state_path": self.bucket_manager.state_path,
                "steps": [
                    {
                        "title": "Input Check",
                        "detail": f"Validated {len(data_bytes)} bytes <= {BLOCK_SIZE_BYTES} byte block size",
                    },
                    {
                        "title": "Bucket Lookup",
                        "detail": f"Read the stored pointer metadata and selected {bucket_name} in O(1) time complexity on the first lookup",
                    },
                    {
                        "title": "Round Robin",
                        "detail": f"Round robin selected block #{block.block_id} from {bucket_name}; next block is " + (f"#{next_block_id}." if next_block_id is not None else "unavailable.") + state_note,
                    },
                    {
                        "title": "Drive Write",
                        "detail": f"Wrote '{data_text[:32]}' to block #{block.block_id} on disk in {latency:.2f} ms",
                    },
                    {
                        "title": "Wear Update",
                        "detail": f"write_count {before_write} -> {block.write_count}; wear accumulates only on write operations",
                    },
                    {
                        "title": "Write Amplification",
                        "detail": f"physical writes +{physical_writes:.2f}; WAF {before_waf:.2f} -> {block.write_amplification_factor:.2f}; health {before_health:.2f} -> {block.health_score:.2f}",
                    },
                    {
                        "title": "Status",
                        "detail": f"Word written successfully; write_count {before_write} -> {block.write_count}; error_count {before_error:.2f} -> {block.error_count:.2f}",
                    },
                ],
            }

        measured, raw_data = self.telemetry.measure_read_data(block.block_id)
        error_sources = self._composite_error_sources(block, include_read_disturb=True)
        error_generated = error_sources["total"]
        before_write = block.write_count
        before_error = block.error_count
        before_health = block.health_score
        ecc_result = self._resolve_read_ecc(block, error_generated)
        status = ecc_result["status"]
        latency = self._read_latency(measured, extra_delay, ecc_result["ecc_latency_cost"])
        block.record_read(latency)
        block.update_latency(latency)
        block.last_error_generated = error_generated
        block.last_ecc_status = ecc_result["stage_used"] if status == "CORRECTED" else status
        block.last_intelligence_triggered = status == "FAILURE"
        block.record_generated_errors(error_generated)

        error_delta = self._read_error_delta(status, error_generated, ecc_result)
        if status == "CORRECTED":
            block.record_error(error_delta)
            block.record_corrected_read()
        elif status == "FAILURE":
            block.record_error(error_delta)
            block.record_failed_read()
        else:
            block.record_error(error_delta)
        old_bucket_name = bucket_name
        block.sync_state()
        self.bucket_manager.sync_block(block)
        pointer_snapshot_after = dict(self.bucket_manager.pointer)
        bucket_snapshot_after = self.bucket_manager.snapshot()
        pointer_after = pointer_snapshot_after.get(old_bucket_name, 0)
        next_block_id = self.bucket_manager.pointer_target(old_bucket_name)
        state_note = ""
        if block.state != old_bucket_name:
            state_note = f" Block moved from {old_bucket_name} to {block.state}, and the stored pointers were updated."

        text_preview = raw_data.decode("utf-8", errors="replace")[:80]
        return {
            "operation": "READ",
            "block_id": block.block_id,
            "bucket": bucket_name,
            "pointer": pointer,
            "pointer_after": pointer_after,
            "status": status,
            "error_generated": error_generated,
            "error_detected": ecc_result["error_detected"],
            "ecc_limit": ECC_STAGES[-1]["capacity"],
            "ecc_stage_used": ecc_result["stage_used"],
            "ecc_stage_attempts": ecc_result["stage_attempts"],
            "verification_passed": ecc_result["verification_passed"],
            "intelligence_triggered": status == "FAILURE",
            "latency": round(latency, 2),
            "write_before": before_write,
            "write_after": block.write_count,
            "error_before": before_error,
            "error_after": round(block.error_count, 2),
            "waf": block.write_amplification_factor,
            "health_score": block.health_score,
            "error_rate": block.error_rate,
            "state": block.state,
            "data_size": len(raw_data),
            "text_preview": text_preview,
            "bucket_snapshot_before": bucket_snapshot_before,
            "bucket_snapshot_after": bucket_snapshot_after,
            "pointer_snapshot_before": pointer_snapshot_before,
            "pointer_snapshot_after": pointer_snapshot_after,
            "bucket_state_path": self.bucket_manager.state_path,
            "steps": [
                {
                    "title": "Block Selection",
                    "detail": f"Manual read selected block #{block.block_id} from {bucket_name}" if block_id is not None else f"Preferred bucket lookup chose {bucket_name}",
                },
                {
                    "title": "Bucket Lookup",
                    "detail": f"Read selection uses {bucket_name} in O(1) time complexity",
                },
                {
                    "title": "Round Robin",
                    "detail": f"Round robin selected block #{block.block_id} from {bucket_name}; next block is " + (f"#{next_block_id}." if next_block_id is not None else "unavailable.") + state_note,
                },
                {
                    "title": "Read",
                    "detail": f"Read {len(raw_data)} bytes from block #{block.block_id} in {latency:.2f} ms",
                },
                {
                    "title": "Error Generation",
                    "detail": f"Wear {error_sources['wear_component']} + retention {error_sources['retention_component']} ({error_sources['retention_age_s']} s idle) + disturb {error_sources['read_disturb_component']} + error-rate {error_sources['error_rate_component']} + latency {error_sources['latency_component']} + health {error_sources['health_penalty']} + WAF {error_sources['waf_component']} + noise {error_sources['random_noise']} => score {error_sources['error_score']} => {error_generated} error unit(s)",
                },
                {
                    "title": "Error Detection",
                    "detail": "Error detected" if ecc_result["error_detected"] else "No error detected; read succeeds without correction",
                },
                {
                    "title": "ECC Check",
                    "detail": f"Risk {ecc_result['risk']:.2f} starts at {ecc_result['start_stage']}; attempts {', '.join(ecc_result['stage_attempts']) or 'none'}; stage used {ecc_result['stage_used']}; status {status}",
                },
                {
                    "title": "Verification",
                    "detail": "Verification passed after correction" if ecc_result["verification_passed"] else ("Verification skipped because no correction was needed" if status == "SUCCESS" else "Verification failed because all ECC stages were exhausted"),
                },
                {
                    "title": "Feedback Loop",
                    "detail": f"error_count {before_error:.2f} -> {block.error_count:.2f}; delta +{error_delta:.2f}; error_rate {block.error_rate:.2f}; health {before_health:.2f} -> {block.health_score:.2f}; latency +{ecc_result['ecc_latency_cost']}; intelligence trigger {'ON' if status == 'FAILURE' else 'OFF'}",
                },
                {
                    "title": "Status",
                    "detail": f"Data available: '{text_preview[:32]}'",
                },
            ],
        }


# DECISION ENGINE
# ─────────────────────────────────────────────────────
class DecisionEngine:
    def __init__(self, blocks: List[BlockState], bucket_manager: BucketManager):
        self.blocks = blocks
        self.bucket_manager = bucket_manager
        self.avoided_blocks: List[int] = []
        self.action_log: deque = deque(maxlen=200)
        self.migration_manager: Optional["MigrationManager"] = None

    def _log(self, msg: str):
        self.action_log.appendleft({"ts": time.time(), "msg": msg})
        log.info(f"[DECISION] {msg}")

    def evaluate_single(self, block: BlockState):
        """Event-driven: evaluate a single block after a read/write."""
        decision = evaluate_block(block)
        old_state = block.intelligence_state
        block.intelligence_state = decision
        block.prev_ecc_errors = block.total_generated_errors
        block.prev_eval_time = time.time()

        if decision == "CRITICAL" and not block.is_avoided:
            block.is_avoided = True
            if block.block_id not in self.avoided_blocks:
                self.avoided_blocks.append(block.block_id)
            block.sync_state()
            self.bucket_manager.sync_block(block)
            self._log(f"[CRITICAL] Block {block.block_id} score={block.risk_score:.2f} -> migrate queued")
            if self.migration_manager:
                self.migration_manager.enqueue(block)
        elif decision == "WARNING" and old_state == "NORMAL":
            self._log(f"[WARNING] Block {block.block_id} score={block.risk_score:.2f} trend={trend_analysis(block)}")
        elif decision == "NORMAL" and old_state == "WARNING":
            self._log(f"[RECOVERED] Block {block.block_id} score={block.risk_score:.2f} back to NORMAL")

    def evaluate(self):
        """Interval-based: evaluate ALL blocks (catches retention aging, drift)."""
        for blk in self.blocks:
            if blk.is_avoided:
                continue
            decision = evaluate_block(blk)
            old_state = blk.intelligence_state
            blk.intelligence_state = decision
            blk.prev_ecc_errors = blk.total_generated_errors
            blk.prev_eval_time = time.time()

            if decision == "CRITICAL" and not blk.is_avoided:
                blk.is_avoided = True
                if blk.block_id not in self.avoided_blocks:
                    self.avoided_blocks.append(blk.block_id)
                blk.sync_state()
                self.bucket_manager.sync_block(blk)
                self._log(f"[CRITICAL] Block {blk.block_id} score={blk.risk_score:.2f} -> migrate queued")
                if self.migration_manager:
                    self.migration_manager.enqueue(blk)
            elif decision == "WARNING" and old_state != "WARNING":
                self._log(f"[WARNING] Block {blk.block_id} score={blk.risk_score:.2f}")
            elif decision == "NORMAL" and old_state == "WARNING":
                self._log(f"[RECOVERED] Block {blk.block_id} score={blk.risk_score:.2f}")

    def top_risky(self, n: int = 5) -> List[dict]:
        active = [b for b in self.blocks if not b.is_avoided]
        ranked = sorted(active, key=lambda b: b.risk_score, reverse=True)
        return [
            {
                "block_id": b.block_id,
                "risk_score": b.risk_score,
                "classification": b.classification,
                "state": b.state,
                "avg_latency": round(b.avg_latency, 2),
                "variance": round(b.latency_variance, 2),
            }
            for b in ranked[:n]
        ]

    def reset(self):
        self.avoided_blocks.clear()
        self.action_log.clear()


class MigrationManager:
    """Priority queue that migrates data from critical blocks to healthy blocks during idle."""

    def __init__(self, telemetry: "Telemetry", blocks: List[BlockState],
                 bucket_manager: BucketManager, decision_engine: DecisionEngine):
        self.telemetry = telemetry
        self.blocks = blocks
        self.bucket_manager = bucket_manager
        self.decision = decision_engine
        self._queue: List[Tuple[int, int]] = []  # (priority, block_id)
        self._in_queue: set = set()
        self.migration_log: deque = deque(maxlen=50)
        self._last_op_tick = 0

    def enqueue(self, block: BlockState):
        if block.block_id in self._in_queue:
            return
        priority = int((1.0 - (block.health_score / 100.0)) * 100)
        heapq.heappush(self._queue, (priority, block.block_id))
        self._in_queue.add(block.block_id)
        log.info(f"[MIGRATE] Queued block {block.block_id} priority={priority}")

    def is_idle(self, system) -> bool:
        if system._paused:
            return True
        recent_events = list(system._rw_events)[:5]
        recent_ops = sum(1 for e in recent_events if e.get("operation") in ("READ", "WRITE"))
        return recent_ops <= 1

    def process_one(self, system) -> Optional[dict]:
        if not self._queue:
            return None
        priority, src_id = heapq.heappop(self._queue)
        self._in_queue.discard(src_id)
        src_block = self.blocks[src_id]

        # Find a healthy target
        target_selection = self.bucket_manager.select_next_detail([STATE_HEALTHY])
        if target_selection is None:
            self.decision._log(f"[MIGRATE-FAIL] No healthy target for block {src_id}")
            return None
        tgt_id, tgt_bucket, _ = target_selection
        tgt_block = self.blocks[tgt_id]

        # Read from source, write to target
        read_lat, data = self.telemetry.measure_read_data(src_id)
        write_lat = self.telemetry.measure_write_data(tgt_id, data)
        total_lat = read_lat + write_lat

        tgt_block.record_write(write_lat)
        tgt_block.record_physical_writes(1.0)

        event = {
            "operation": "MIGRATE",
            "source_block": src_id,
            "target_block": tgt_id,
            "priority": priority,
            "latency": round(total_lat, 2),
            "source_health": src_block.health_score,
            "target_health": tgt_block.health_score,
            "status": "MIGRATED",
            "steps": [
                {"title": "Migration Trigger", "detail": f"Block #{src_id} reached CRITICAL (priority {priority})"},
                {"title": "Target Selection", "detail": f"Selected healthy block #{tgt_id} from {tgt_bucket}"},
                {"title": "Data Transfer", "detail": f"Read {len(data)} bytes from #{src_id}, wrote to #{tgt_id} in {total_lat:.2f}ms"},
                {"title": "Source Retired", "detail": f"Block #{src_id} marked RETIRED after successful migration"},
            ],
        }
        self.migration_log.appendleft(event)
        self.decision._log(f"[MIGRATE] Block #{src_id} (pri={priority}) -> #{tgt_id} in {total_lat:.2f}ms")
        return event

    def run_if_idle(self, system) -> List[dict]:
        if not self.is_idle(system) or not self._queue:
            return []
        events = []
        for _ in range(min(MIGRATION_BATCH, len(self._queue))):
            evt = self.process_one(system)
            if evt:
                events.append(evt)
        return events

    def reset(self):
        self._queue.clear()
        self._in_queue.clear()
        self.migration_log.clear()

    @property
    def queue_size(self) -> int:
        return len(self._queue)

    @property
    def pending_blocks(self) -> List[dict]:
        return [{"priority": p, "block_id": bid} for p, bid in sorted(self._queue)]

# MAIN SYSTEM
# ─────────────────────────────────────────────────────
class StorageGuardSystem:
    def __init__(self, base_path: str):
        self.blocks: List[BlockState] = [BlockState(block_id=i) for i in range(NUM_BLOCKS)]
        self.block_map: Dict[int, BlockState] = {block.block_id: block for block in self.blocks}
        self.telemetry = Telemetry(base_path)
        self.metadata_path = os.path.join(base_path, "metadata.json")
        self._load_metadata()
        self.bucket_manager = BucketManager(self.blocks, base_path)
        self.decision = DecisionEngine(self.blocks, self.bucket_manager)
        self.migration_manager = MigrationManager(self.telemetry, self.blocks, self.bucket_manager, self.decision)
        self.decision.migration_manager = self.migration_manager
        self.stress = StressEngine(self.telemetry, self.blocks, self.bucket_manager)
        self.base_path = base_path

        self._tick = 0
        self._total_writes = 0
        self._total_reads = 0
        self._avoided_writes = 0
        self._trad_latency = 0.0
        self._prop_latency = 0.0
        self._paused = False
        self._rw_events: deque = deque(maxlen=20)
        self._manual_sim_event: Optional[dict] = None
        self._manual_event_id = 0

        # Scheduled controlled fault injections
        self._inject_schedule = {10: (4, 35.0), 22: (11, 50.0), 38: (19, 70.0)}
        self._persist_metadata()

    def _persist_metadata(self):
        payload = {
            "updated_at": time.time(),
            "num_blocks": NUM_BLOCKS,
            "block_size_bytes": BLOCK_SIZE_BYTES,
            "blocks": [block.to_metadata() for block in self.blocks],
        }
        tmp_path = f"{self.metadata_path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        os.replace(tmp_path, self.metadata_path)

    def _load_metadata(self):
        if not os.path.exists(self.metadata_path):
            return
        try:
            with open(self.metadata_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            blocks = payload.get("blocks", [])
            for block_payload in blocks:
                block_id = int(block_payload.get("block_id", -1))
                block = self.block_map.get(block_id)
                if block is None:
                    continue
                block.apply_metadata(block_payload)
            log.info(f"[METADATA] Loaded block metadata from {self.metadata_path}")
        except Exception as exc:
            log.warning(f"[METADATA] Failed to load {self.metadata_path}: {exc}")

    def tick(self) -> dict:
        if self._paused:
            migration_events = self.migration_manager.run_if_idle(self)
            for evt in migration_events:
                self._rw_events.appendleft(evt)
            self.bucket_manager.sync_all(self.blocks)
            return self._build_payload()

        self._tick += 1
        if self._tick in self._inject_schedule:
            blk, delay = self._inject_schedule[self._tick]
            self.stress.inject_degradation(blk, delay)

        results = self.stress.run_tick()
        for event in results:
            self._rw_events.appendleft(event)

        # Interval-based evaluation (catches aging/drift)
        if self._tick % EVAL_INTERVAL_TICKS == 0:
            self.decision.evaluate()

        # Try to run migrations if idle
        migration_events = self.migration_manager.run_if_idle(self)
        for evt in migration_events:
            self._rw_events.appendleft(evt)

        self.bucket_manager.sync_all(self.blocks)

        for event in results:
            blk_id = event["block_id"]
            op = event["operation"].lower()
            lat = event["latency"]
            block = self.block_map[blk_id]
            if op == "write":
                self._total_writes += 1
                if block.is_avoided:
                    self._avoided_writes += 1
            else:
                self._total_reads += 1
            self._trad_latency += lat
            self._prop_latency += (5.0 if block.is_avoided else lat)

        self._persist_metadata()
        return self._build_payload()

    def _build_payload(self) -> dict:
        blocks_data = [
            {
                "id": b.block_id,
                "risk": b.risk_score,
                "health_score_v2": b.health_score,  # new 0-100 score
                "cls": b.classification,
                "intelligence_state": b.intelligence_state, # NORMAL/WARNING/CRITICAL
                "state": b.state,
                "write_count": b.write_count,
                "error_count": round(b.error_count, 2),
                "latency": round(b.latency, 2),
                "avg_lat": round(b.avg_latency, 2),
                "variance": round(b.latency_variance, 2),
                "trend": round(b.recent_trend, 4),
                "wear_component": round(b.wear_component, 4),
                "error_rate": round(b.error_rate, 4),
                "ecc_correction_rate": round(b.ecc_correction_rate, 4),
                "health_score": b.health_score, # backward compat
                "waf": round(b.write_amplification_factor, 4),
                "access": b.access_count,
                "bucket": self.bucket_manager.block_bucket.get(b.block_id, b.state),
                "avoided": b.is_avoided,
                "injected": b.degradation_injected,
                "last_operation": b.last_operation,
                "last_error_generated": b.last_error_generated,
                "last_ecc_status": b.last_ecc_status,
                "last_intelligence_triggered": b.last_intelligence_triggered,
                "features": extract_features(b),
                "thresholds": dynamic_thresholds(b)
            }
            for b in self.blocks
        ]

        all_readings = []
        for b in self.blocks:
            all_readings.extend(list(b.latency_history)[-5:])
        lat_series = [round(x, 2) for x in all_readings[-60:]]

        action_log = [{"ts": e["ts"], "msg": e["msg"]} for e in list(self.decision.action_log)[:20]]

        ops = self._total_writes + self._total_reads
        return {
            "tick": self._tick,
            "ts": time.time(),
            "block_size_bytes": BLOCK_SIZE_BYTES,
            "blocks": blocks_data,
            "buckets": self.bucket_manager.snapshot(),
            "bucket_pointers": dict(self.bucket_manager.pointer),
            "bucket_state_path": self.bucket_manager.state_path,
            "metadata_path": self.metadata_path,
            "top_risky": self.decision.top_risky(5),
            "rw_events": list(self._rw_events)[:10],
            "manual_sim_event": self._manual_sim_event,
            "latency_series": lat_series,
            "action_log": action_log,
            "sd_path": self.base_path,
            "paused": self._paused,
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
        if block_id in self.block_map:
            self.stress.inject_degradation(block_id, delay_ms)

    def run_manual_simulation(self, operation: str, data_text: str = "", block_id: Optional[int] = None):
        event = self.stress.run_manual_operation(operation, data_text, block_id)
        self._manual_event_id += 1
        event["event_id"] = self._manual_event_id
        self._manual_sim_event = event
        self._rw_events.appendleft(event)
        self.bucket_manager.sync_all(self.blocks)

        event["bucket_snapshot_after"] = self.bucket_manager.snapshot()
        event["pointer_snapshot_after"] = dict(self.bucket_manager.pointer)
        event["bucket_state_path"] = self.bucket_manager.state_path

        if event["block_id"] is None:
            return

        block = self.block_map[event["block_id"]]
        if operation.upper() == "WRITE":
            self._total_writes += 1
            if block.is_avoided:
                self._avoided_writes += 1
        else:
            self._total_reads += 1

        lat = event["latency"]
        self._trad_latency += lat
        self._prop_latency += (5.0 if block.is_avoided else lat)
        self._persist_metadata()

    def toggle_pause(self):
        self._paused = not self._paused
        log.info(f"[PAUSE] Simulation {'paused' if self._paused else 'running'}")

    def set_paused(self, paused: bool):
        self._paused = bool(paused)
        log.info(f"[PAUSE] Simulation {'paused' if self._paused else 'running'}")

    def cleanup(self):
        self.telemetry.cleanup()

    def reset(self):
        self._tick = 0
        self._total_writes = 0
        self._total_reads = 0
        self._avoided_writes = 0
        self._trad_latency = 0.0
        self._prop_latency = 0.0
        self._paused = False
        self._rw_events.clear()
        self._manual_sim_event = None

        for block in self.blocks:
            block.reset_runtime()

        self.telemetry.reset()
        self.stress.reset()
        self.decision.reset()
        self.bucket_manager.reset(self.blocks)
        self._persist_metadata()
        self._rw_events.clear()
        self._manual_sim_event = None
        log.info("[RESET] Simulation state cleared")

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
                    elif t == "reset":
                        system.reset()
                    elif t == "toggle_pause":
                        system.toggle_pause()
                    elif t == "manual_simulate":
                        system.run_manual_simulation(cmd["operation"], cmd.get("data", ""), cmd.get("block_id"))
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
