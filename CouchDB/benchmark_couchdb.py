import time
import psutil
import requests
import pandas as pd
from statistics import mean
from concurrent.futures import ThreadPoolExecutor

# ========================= CONFIG ============================
COUCH_URL = "http://127.0.0.1:5984"
USERNAME = "ivan"
PASSWORD = "sigma"

DBS = ["orders", "transactions", "products", "sellers"]
THREADS = [10, 30, 50]   # SAFE for CouchDB on Windows
# ============================================================

# --------------------------------
# SESSION (IMPORTANT)
# --------------------------------
session = requests.Session()
session.auth = (USERNAME, PASSWORD)

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
def point_read(db):
    r = session.get(
        f"{COUCH_URL}/{db}/_all_docs?limit=1&include_docs=true"
    )
    return r.json()

def scan_read(db, limit=300):
    r = session.get(
        f"{COUCH_URL}/{db}/_all_docs?limit={limit}&include_docs=true"
    )
    return r.json()

def insert_doc(db):
    base = point_read(db)["rows"][0]["doc"]
    base.pop("_id", None)
    base.pop("_rev", None)

    r = session.post(
        f"{COUCH_URL}/{db}",
        json=base
    )
    return r.json()

def update_doc(db):
    doc = point_read(db)["rows"][0]["doc"]
    doc["__bench_update"] = doc.get("__bench_update", 0) + 1

    r = session.put(
        f"{COUCH_URL}/{db}/{doc['_id']}",
        json=doc
    )
    return r.json()

# --------------------------------
# ADD-TO-CART (COMPOSITE WORKLOAD)
# --------------------------------
def add_to_cart():
    product = session.get(
        f"{COUCH_URL}/products/_all_docs?limit=1&include_docs=true"
    ).json()["rows"][0]["doc"]

    order = session.get(
        f"{COUCH_URL}/orders/_all_docs?limit=1&include_docs=true"
    ).json()["rows"][0]["doc"]

    order["cart_items"] = order.get("cart_items", 0) + 1

    r = session.put(
        f"{COUCH_URL}/orders/{order['_id']}",
        json=order
    )
    return r.json()

# --------------------------------
# SCALABILITY FUNCTIONS
# --------------------------------
def threaded_latency(func, db, n_threads):
    def task():
        func(db)

    start = time.time()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        for _ in range(n_threads):
            ex.submit(task)
    return (time.time() - start) / n_threads

def threaded_throughput(func, db, n_threads, duration=5):
    count = 0
    start = time.time()

    def task():
        nonlocal count
        while time.time() - start < duration:
            func(db)
            count += 1

    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        for _ in range(n_threads):
            ex.submit(task)

    return count / duration

# --------------------------------
# RUN BENCHMARKS
# --------------------------------
results = []

print("\nRunning CouchDB Benchmarks...\n")

for db in DBS:
    print(f"Dataset: {db}")

    results.extend([
        ["CouchDB", db, "Read latency", measure(lambda: point_read(db)) * 1000, None],
        ["CouchDB", db, "Scan latency", measure(lambda: scan_read(db)) * 1000, None],
        ["CouchDB", db, "Insert latency", measure(lambda: insert_doc(db)) * 1000, None],
        ["CouchDB", db, "Update latency", measure(lambda: update_doc(db)) * 1000, None],
    ])

    results.append(
        ["CouchDB", db, "Throughput", None, throughput(lambda: point_read(db))]
    )

    for t in THREADS:
        lat = threaded_latency(point_read, db, t)
        tput = threaded_throughput(point_read, db, t)

        results.append(
            ["CouchDB", db, f"Read latency ({t} threads)", lat * 1000, None]
        )
        results.append(
            ["CouchDB", db, f"Throughput ({t} threads)", None, tput]
        )

    print(f"  Completed CRUD + scalability for {db}")

# --------------------------------
# ADD-TO-CART METRICS
# --------------------------------
print("\nRunning Add-to-Cart Benchmark...\n")

results.append(
    ["CouchDB", "orders", "Add-to-Cart latency",
     measure(add_to_cart) * 1000, None]
)

results.append(
    ["CouchDB", "orders", "Add-to-Cart throughput",
     None, throughput(add_to_cart)]
)

# --------------------------------
# MEMORY USAGE
# --------------------------------
ram = psutil.Process().memory_info().rss / (1024 ** 2)
results.append(["CouchDB", "System", "RAM usage (MB)", ram, None])

# --------------------------------
# SAVE RESULTS
# --------------------------------
df = pd.DataFrame(
    results,
    columns=["Database", "Dataset", "Metric", "Latency (ms)", "Throughput (ops/sec)"]
)
df.to_csv("couchdb_metrics_full.csv", index=False)

print("\nSaved results to couchdb_metrics_full.csv")
print("Benchmark completed.")
