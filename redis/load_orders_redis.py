import redis
import pandas as pd
import os

# -------------------------------
# REDIS CONNECTION
# -------------------------------
r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

# -------------------------------
# DATASET PATH (UPDATE IF NEEDED)
# -------------------------------
csv_path = r"C:\Users\Gyjyv\OneDrive\Documents\Assignments\DBMS Sem9\online_retail_II.csv"

# Safety check
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

print("CSV file found. Loading orders data...")

# -------------------------------
# LOAD CSV (ROBUST PARSING)
# -------------------------------
df = pd.read_csv(
    csv_path,
    engine="python",
    on_bad_lines="skip"
)

print(f"Loaded {len(df)} order records")

# -------------------------------
# INSERT INTO REDIS
# -------------------------------
inserted = 0

for idx, row in df.iterrows():
    # Use index as fallback ID if InvoiceNo is missing
    order_id = row.get("InvoiceNo", idx)
    key = f"order:{order_id}"

    # Convert row to Redis-safe dictionary
    data = {k: str(v) for k, v in row.items() if pd.notna(v)}

    r.hset(key, mapping=data)
    inserted += 1

print(f"Inserted {inserted} orders into Redis")

# -------------------------------
# VERIFICATION
# -------------------------------
sample_keys = r.keys("order:*")
print(f"Total order keys in Redis: {len(sample_keys)}")

if sample_keys:
    print("Sample order record:")
    print(r.hgetall(sample_keys[0]))

print("✅ Orders successfully loaded into Redis.")
