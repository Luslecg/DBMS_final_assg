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
# DATASET PATH (OPTION 1)
# -------------------------------
csv_path = r"C:\Users\Gyjyv\OneDrive\Documents\Assignments\DBMS Sem9\styles.csv"

# Safety check
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

print("CSV file found. Loading data...")

# -------------------------------
# LOAD CSV
# -------------------------------
df = pd.read_csv(
    csv_path,
    engine="python",
    on_bad_lines="skip"
)

print(f"Loaded {len(df)} product records")

# -------------------------------
# INSERT INTO REDIS
# -------------------------------
inserted = 0

for _, row in df.iterrows():
    product_id = row["id"]
    key = f"product:{product_id}"

    # Convert row to Redis-safe dictionary
    data = {k: str(v) for k, v in row.items() if pd.notna(v)}

    r.hset(key, mapping=data)
    inserted += 1

print(f"Inserted {inserted} products into Redis")

# -------------------------------
# VERIFICATION
# -------------------------------
sample_keys = r.keys("product:*")
print(f"Total product keys in Redis: {len(sample_keys)}")

if sample_keys:
    print("Sample product record:")
    print(r.hgetall(sample_keys[0]))

print("✅ Products successfully loaded into Redis.")
