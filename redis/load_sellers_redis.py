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
csv_path = r"C:\Users\Gyjyv\OneDrive\Documents\Assignments\DBMS Sem9\olist_sellers_dataset.csv"

# Safety check
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

print("CSV file found. Loading sellers data...")

# -------------------------------
# LOAD CSV
# -------------------------------
df = pd.read_csv(csv_path)
print(f"Loaded {len(df)} seller records")

# -------------------------------
# INSERT INTO REDIS
# -------------------------------
inserted = 0

for _, row in df.iterrows():
    seller_id = row["seller_id"]
    key = f"seller:{seller_id}"

    data = {k: str(v) for k, v in row.items() if pd.notna(v)}
    r.hset(key, mapping=data)
    inserted += 1

print(f"Inserted {inserted} sellers into Redis")

# -------------------------------
# VERIFICATION
# -------------------------------
sample_keys = r.keys("seller:*")
print(f"Total seller keys in Redis: {len(sample_keys)}")

if sample_keys:
    print("Sample seller record:")
    print(r.hgetall(sample_keys[0]))

print("✅ Sellers successfully loaded into Redis.")
