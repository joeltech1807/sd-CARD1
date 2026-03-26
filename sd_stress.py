import os
import time
import random

print("STARTING SCRIPT...", flush=True)

FILE = "G:/test.bin"
SIZE_MB = 20   # smaller for faster start
BLOCK = 4096
NUM_BLOCKS = 20

# Create file if not exists
if not os.path.exists(FILE):
    print("Creating file...", flush=True)
    with open(FILE, "wb") as f:
        f.write(os.urandom(SIZE_MB * 1024 * 1024))
    print("File created!", flush=True)

print("Opening file...", flush=True)

# Open file
f = open(FILE, "r+b")

block_size = (SIZE_MB * 1024 * 1024) // NUM_BLOCKS

block_latency = {i: [] for i in range(NUM_BLOCKS)}
block_status = {i: "HEALTHY" for i in range(NUM_BLOCKS)}

print("Entering loop...\n", flush=True)

while True:
    # Choose random position
    pos = random.randint(0, SIZE_MB * 1024 * 1024 - BLOCK)
    block_id = pos // block_size

    start = time.time()

    # WRITE
    f.seek(pos)
    f.write(os.urandom(BLOCK))

    # READ
    f.seek(pos)
    _ = f.read(BLOCK)

    # Force write to disk
    f.flush()
    os.fsync(f.fileno())

    latency = time.time() - start

    # Store latency
    block_latency[block_id].append(latency)

    if len(block_latency[block_id]) > 20:
        block_latency[block_id].pop(0)

    avg_latency = sum(block_latency[block_id]) / len(block_latency[block_id])

    # Classification
    if avg_latency < 0.002:
        status = "HEALTHY"
    elif avg_latency < 0.005:
        status = "RISKY"
    else:
        status = "CRITICAL"

    block_status[block_id] = status

    # Artificial degradation for demo
    if block_id == 5:
        time.sleep(0.05)

    # Print output (FORCED)
    print(
        f"Block {block_id} | Latency: {latency:.4f}s | Status: {status}",
        flush=True
    )

    # Slow down loop for visibility
    time.sleep(0.1)