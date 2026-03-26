# StorageGuard — Predictive Storage Reliability System
================================================

## Files

| File | Purpose |
|------|---------|
| `storageguard_dashboard.html` | Self-contained dashboard (runs standalone in any browser) |
| `storage_backend.py` | Python backend — real file I/O on SD card / temp dir |

---

## Quick Start (Browser-Only / No SD Card)

Just open `storageguard_dashboard.html` in any modern browser.  
The built-in JavaScript simulation engine runs immediately — no server needed.

---

## With Real SD Card (Python Backend)

### 1. Install dependencies
```bash
pip install websockets
```

### 2. Run backend
```bash
# Point to your SD card mount (e.g. /media/sdcard or D:\ on Windows)
python storage_backend.py --path /media/sdcard --port 8765

# Falls back to a temp directory if no path given:
python storage_backend.py
```

### 3. Open dashboard
Open `storageguard_dashboard.html` in your browser.  
The dashboard auto-connects to `ws://localhost:8765`.  
The status badge switches from **SIMULATING** → **CONNECTED**.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  storage_backend.py                  │
│                                                     │
│  ┌───────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Telemetry │→ │ StressEngine │→ │DecisionEngine│ │
│  │ (file I/O)│  │(workload gen)│  │(risk + avoid)│ │
│  └───────────┘  └──────────────┘  └──────────────┘ │
│                        │ WebSocket JSON tick         │
└────────────────────────┼────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│             storageguard_dashboard.html              │
│                                                     │
│  Block Grid │ Prediction Panel                       │
│  Latency Graph │ Action Log                          │
│  Comparison Panel (Traditional vs Predictive)        │
└─────────────────────────────────────────────────────┘
```

---

## System Design

### Telemetry
- Measures actual `open/write/fsync/read` time in milliseconds
- 32 logical blocks × 64 KB each
- Per-block: avg latency, variance, trend, access count

### Risk Model (data-driven, no hardcoding)
```
risk = 0.45 × (avg_lat / 80ms)
     + 0.35 × (std_dev / 30ms)
     + 0.20 × (recent trend 0–1)
```
- `< 0.40` → NORMAL
- `0.40 – 0.70` → ELEVATED
- `> 0.70` → CRITICAL → writes redirected

### Stress Engine
- Modes: `mixed`, `random`, `sequential`
- Intensity: 1–10 (controls ops/tick)
- Fault injection: per-block delay injection

### Decision Engine
- Marks CRITICAL blocks as avoided
- Redirects writes to healthy blocks
- Auto-recovers if risk drops below 32% (ELEVATED × 0.8)

### Comparison Panel
- **Traditional**: cumulates real I/O latency for every block
- **Predictive**: substitutes ~5 ms for avoided-block operations
- Savings shown as `Δ ms/op` and `%`

---

## Controlled Fault Injection Schedule (default)

| Tick | Block | Extra delay |
|------|-------|-------------|
| 12   | 4     | +38 ms      |
| 22   | 11    | +55 ms      |
| 36   | 19    | +72 ms      |

Or inject manually via the **⚡ Inject Fault** button in the UI.

---

## WebSocket Protocol

**Server → Client** (every 0.5s):
```json
{
  "tick": 42,
  "blocks": [{ "id": 0, "risk": 0.12, "cls": "NORMAL", "avg_lat": 7.3, ... }],
  "top_risky": [...],
  "latency_series": [5.1, 8.2, 62.4, ...],
  "action_log": [{ "ts": 1234567890.1, "msg": "Block 4 → CRITICAL" }],
  "stats": { "trad_avg_lat": 18.4, "prop_avg_lat": 9.1, "avoided_writes": 34, ... }
}
```

**Client → Server** (controls):
```json
{ "type": "set_mode",      "value": "random" }
{ "type": "set_intensity", "value": 0.7 }
{ "type": "inject",        "block": 5, "delay": 60 }
```
