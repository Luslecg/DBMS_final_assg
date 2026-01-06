import redis
import time
from statistics import mean

# -------------------------------
# REDIS CONNECTION
# -------------------------------
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

# -------------------------------
# PREPARE SAMPLE KEYS
# -------------------------------
product_keys = r.keys("product:*")
order_keys = r.keys("order:*")

if not product_keys or not order_keys:
    raise RuntimeError("Products or Orders not found in Redis")

product_key = product_keys[0]
order_key = order_keys[0]

print("Using:")
print("Product:", product_key)
print("Order:", order_key)

# -------------------------------
# ADD-TO-CART OPERATION
# -------------------------------
def add_to_cart():
    # Read product (simulate lookup)
    r.hgetall(product_key)

    # Update order cart
    r.hincrby(order_key, "cart_items", 1)

# -------------------------------
# LATENCY MEASUREMENT
# -------------------------------
def measure_latency(runs=1000):
    times = []
    for _ in range(runs):
        start = time.time()
        add_to_cart()
        times.append((time.time() - start) * 1000)
    return mean(times)

# -------------------------------
# THROUGHPUT MEASUREMENT
# -------------------------------
def measure_throughput(duration=5):
    count = 0
    start = time.time()
    while time.time() - start < duration:
        add_to_cart()
        count += 1
    return count / duration

# -------------------------------
# RUN BENCHMARK
# -------------------------------
print("\nRunning Redis Add-to-Cart benchmark...\n")

latency_ms = measure_latency()
throughput_ops = measure_throughput()

print(f"Add-to-Cart Latency (ms): {latency_ms:.4f}")
print(f"Add-to-Cart Throughput (ops/sec): {throughput_ops:.2f}")

# -------------------------------
# SAVE RESULTS
# -------------------------------
import pandas as pd

df = pd.DataFrame([{
    "Database": "Redis",
    "Dataset": "orders",
    "Metric": "Add-to-Cart latency",
    "Latency (ms)": latency_ms,
    "Throughput (ops/sec)": None
}, {
    "Database": "Redis",
    "Dataset": "orders",
    "Metric": "Add-to-Cart throughput",
    "Latency (ms)": None,
    "Throughput (ops/sec)": throughput_ops
}])

df.to_csv("redis_add_to_cart_metrics.csv", index=False)

print("\nSaved results to redis_add_to_cart_metrics.csv")
print("Benchmark completed.")
