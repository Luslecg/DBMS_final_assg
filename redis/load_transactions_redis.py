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
csv_path = r"C:\Users\Gyjyv\OneDrive\Documents\Assignments\DBMS Sem9\data.csv"

# Safety check
if not os.path.exists(csv_path):
    raise FileNotFoundError(f"CSV file not found: {csv_path}")

print("CSV file found. Loading transactions data...")

# -------------------------------
# LOAD CSV (ROBUST PARSING)
# -------------------------------
df = pd.read_csv(
    csv_path,
    engine="python",
    on_bad_lines="skip"
)

print(f"Loaded {len(df)} transaction records")

# -------------------------------
# INSERT INTO REDIS
# -------------------------------
inserted = 0

for idx, row in df.iterrows():
    # Use index as unique transaction ID
    txn_id = row.get("InvoiceNo", idx)
    key = f"transaction:{txn_id}"

    data = {k: str(v) for k, v in row.items() if pd.notna(v)}
    r.hset(key, mapping=data)
    inserted += 1

print(f"Inserted {inserted} transactions into Redis")

# -------------------------------
# VERIFICATION
# -------------------------------
sample_keys = r.keys("transaction:*")
print(f"Total transaction keys in Redis: {len(sample_keys)}")

if sample_keys:
    print("Sample transaction record:")
    print(r.hgetall(sample_keys[0]))

print("✅ Transactions successfully loaded into Redis.")
