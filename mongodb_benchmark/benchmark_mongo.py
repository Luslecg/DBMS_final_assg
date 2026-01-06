import time
import psutil
import pymongo
import pandas as pd
from statistics import mean
from concurrent.futures import ThreadPoolExecutor

# --------------------------------
# CONNECT TO MONGODB
# --------------------------------
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_db"]

COLLECTIONS = {
    "orders": db.orders,
    "transactions": db.transactions,
    "products": db.products,
    "sellers": db.sellers,
}

THREADS = [10, 50, 100, 200]

# --------------------------------
# GENERIC MEASUREMENT
# --------------------------------
def measure(func, runs=15):
    times = []
    for _ in range(runs):
        start = time.time()
        func()
        times.append(time.time() - start)
    return mean(times)

def throughput(func, duration=5):
    count = 0
    start = time.time()
    while time.time() - start < duration:
        func()
        count += 1
    return count / duration

# --------------------------------
# BASIC CRUD OPERATIONS
# --------------------------------
def point_read(col):
    return col.find_one()

def scan_read(col):
    return list(col.find().limit(300))

def insert_doc(col):
    doc = col.find_one()
    doc.pop("_id")
    return col.insert_one(doc)

def update_doc(col):
    doc = col.find_one()
    return col.update_one(
        {"_id": doc["_id"]},
        {"$inc": {"__bench_update": 1}}
    )

# --------------------------------
# ADD-TO-CART (COMPOSITE WORKLOAD)
# --------------------------------
def add_to_cart():
    product = db.products.find_one()
    order = db.orders.find_one()
    return db.orders.update_one(
        {"_id": order["_id"]},
        {"$inc": {"cart_items": 1}}
    )

# --------------------------------
# SCALABILITY FUNCTIONS
# --------------------------------
def threaded_latency(func, col, n_threads):
    def task():
        func(col)

    start = time.time()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        for _ in range(n_threads):
            ex.submit(task)
    return (time.time() - start) / n_threads

def threaded_throughput(func, col, n_threads, duration=5):
    count = 0
    start = time.time()

    def task():
        nonlocal count
        while time.time() - start < duration:
            func(col)
            count += 1

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        for _ in range(n_threads):
            ex.submit(task)

    return count / duration

# --------------------------------
# RUN BENCHMARKS
# --------------------------------
results = []

print("\nRunning MongoDB Benchmarks...\n")

for name, col in COLLECTIONS.items():
    print(f"Dataset: {name}")

    # CRUD latency
    results.extend([
        ["MongoDB", name, "Read latency", measure(lambda: point_read(col)) * 1000, None],
        ["MongoDB", name, "Scan latency", measure(lambda: scan_read(col)) * 1000, None],
        ["MongoDB", name, "Insert latency", measure(lambda: insert_doc(col)) * 1000, None],
        ["MongoDB", name, "Update latency", measure(lambda: update_doc(col)) * 1000, None],
    ])

    # CRUD throughput
    results.append(
        ["MongoDB", name, "Throughput", None, throughput(lambda: point_read(col))]
    )

    # Scalability (read)
    for t in THREADS:
        lat = threaded_latency(point_read, col, t)
        tput = threaded_throughput(point_read, col, t)

        results.append(
            ["MongoDB", name, f"Read latency ({t} threads)", lat * 1000, None]
        )
        results.append(
            ["MongoDB", name, f"Throughput ({t} threads)", None, tput]
        )

    print(f"  Completed CRUD + scalability for {name}")

# --------------------------------
# ADD-TO-CART METRICS (GLOBAL)
# --------------------------------
print("\nRunning Add-to-Cart Benchmark...\n")

results.append(
    ["MongoDB", "Orders", "Add-to-Cart latency",
     measure(add_to_cart) * 1000, None]
)

results.append(
    ["MongoDB", "Orders", "Add-to-Cart throughput",
     None, throughput(add_to_cart)]
)

# --------------------------------
# MEMORY USAGE
# --------------------------------
ram = psutil.Process().memory_info().rss / (1024 ** 2)
results.append(["MongoDB", "System", "RAM usage (MB)", ram, None])

# --------------------------------
# SAVE RESULTS
# --------------------------------
df = pd.DataFrame(
    results,
    columns=["Database", "Dataset", "Metric", "Latency (ms)", "Throughput (ops/sec)"]
)
df.to_csv("mongo_metrics_full.csv", index=False)

print("\nSaved results to mongo_metrics_full.csv")
print("Benchmark completed.")
