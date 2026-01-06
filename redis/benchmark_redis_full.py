import redis
import time
import psutil
import pandas as pd
from statistics import mean
from concurrent.futures import ThreadPoolExecutor

# -------------------------------
# REDIS CONNECTION
# -------------------------------
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

DATASETS = {
    "products": "product:*",
    "orders": "order:*",
    "transactions": "transaction:*",
    "sellers": "seller:*"
}

THREADS = [10, 50, 100, 200]
RUNS = 15
SCAN_LIMIT = 300

results = []

# -------------------------------
# GENERIC MEASUREMENTS
# -------------------------------
def measure_latency(func, runs=RUNS):
    times = []
    for _ in range(runs):
        start = time.time()
        func()
        times.append((time.time() - start) * 1000)
    return mean(times)

def measure_throughput(func, duration=5):
    count = 0
    start = time.time()
    while time.time() - start < duration:
        func()
        count += 1
    return count / duration

# -------------------------------
# BASIC OPERATIONS
# -------------------------------
def get_sample_key(pattern):
    keys = list(r.scan_iter(match=pattern, count=1))
    return keys[0] if keys else None

def point_read(key):
    r.hgetall(key)

def scan_read(pattern):
    for _, k in zip(range(SCAN_LIMIT), r.scan_iter(match=pattern)):
        r.hgetall(k)

def insert_clone(key, prefix):
    data = r.hgetall(key)
    new_key = f"{prefix}:clone:{int(time.time() * 1000)}"
    r.hset(new_key, mapping=data)

def update_doc(key):
    r.hincrby(key, "__bench_update", 1)

# -------------------------------
# SCALABILITY
# -------------------------------
def threaded_latency(func, key, n_threads):
    def task():
        func(key)

    start = time.time()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        for _ in range(n_threads):
            ex.submit(task)
    return ((time.time() - start) / n_threads) * 1000

def threaded_throughput(func, key, n_threads, duration=5):
    count = 0
    start = time.time()

    def task():
        nonlocal count
        while time.time() - start < duration:
            func(key)
            count += 1

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        for _ in range(n_threads):
            ex.submit(task)

    return count / duration

# -------------------------------
# RUN BENCHMARKS
# -------------------------------
print("\nRunning Redis Full Benchmarks (Non-Cart)...\n")

for dataset, pattern in DATASETS.items():
    print(f"Dataset: {dataset}")

    sample_key = get_sample_key(pattern)
    if not sample_key:
        print(f"  No keys found for {dataset}, skipping.")
        continue

    # Latency
    results.extend([
        ["Redis", dataset, "Read latency", measure_latency(lambda: point_read(sample_key)), None],
        ["Redis", dataset, "Scan latency", measure_latency(lambda: scan_read(pattern)), None],
        ["Redis", dataset, "Insert latency", measure_latency(lambda: insert_clone(sample_key, dataset)), None],
        ["Redis", dataset, "Update latency", measure_latency(lambda: update_doc(sample_key)), None],
    ])

    # Throughput
    results.append(
        ["Redis", dataset, "Throughput", None, measure_throughput(lambda: point_read(sample_key))]
    )

    # Scalability
    for t in THREADS:
        lat = threaded_latency(point_read, sample_key, t)
        tput = threaded_throughput(point_read, sample_key, t)

        results.append(
            ["Redis", dataset, f"Read latency ({t} threads)", lat, None]
        )
        results.append(
            ["Redis", dataset, f"Throughput ({t} threads)", None, tput]
        )

    print(f"  Completed CRUD + scalability for {dataset}")

# -------------------------------
# MEMORY USAGE
# -------------------------------
ram_mb = psutil.Process().memory_info().rss / (1024 ** 2)
results.append(["Redis", "System", "RAM usage (MB)", ram_mb, None])

# -------------------------------
# SAVE RESULTS
# -------------------------------
df = pd.DataFrame(
    results,
    columns=["Database", "Dataset", "Metric", "Latency (ms)", "Throughput (ops/sec)"]
)

df.to_csv("redis_full_metrics.csv", index=False)

print("\nSaved results to redis_full_metrics.csv")
print("Redis full benchmark completed.")
