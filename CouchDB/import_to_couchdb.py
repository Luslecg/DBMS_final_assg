import csv
import json
import os
import requests


# ========================= CONFIG ============================
COUCH_URL = "http://127.0.0.1:5984"
USERNAME = "ivan"      # UPDATE THIS
PASSWORD = "sigma"   # UPDATE THIS

DATA_FOLDER = r"c:\Users\Gyjyv\Downloads\AI lab test"  # UPDATE THIS

DATASETS = {
    "online_retail_II.csv": ("orders", "Invoice"),
    "data.csv": ("transactions", "InvoiceNo"),
    "styles.csv": ("products", "id"),
    "olist_sellers_dataset.csv": ("sellers", "seller_id"),
}
# =============================================================


def create_db(db_name):
    url = f"{COUCH_URL}/{db_name}"
    r = requests.get(url, auth=(USERNAME, PASSWORD))
    if r.status_code == 200:
        print(f"Database '{db_name}' already exists.")
    else:
        r = requests.put(url, auth=(USERNAME, PASSWORD))
        print(f"Created database '{db_name}'." if r.status_code in (200, 201)
              else f"ERROR creating DB {db_name}: {r.text}")


def try_convert(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except:
        pass
    try:
        return float(v)
    except:
        pass
    return v


def load_csv_to_docs(path, id_field):
    docs = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc = {k: try_convert(v) for k, v in row.items()}
            if id_field not in doc or doc[id_field] is None:
                raise ValueError(f"Missing key '{id_field}' in row: {row}")
            doc["_id"] = str(doc[id_field])
            docs.append(doc)
    print(f"Loaded {len(docs)} docs from {os.path.basename(path)}")
    return docs


def bulk_insert(db, docs, batch_size=5000):
    url = f"{COUCH_URL}/{db}/_bulk_docs"
    
    total = len(docs)
    print(f"Inserting {total} documents into '{db}' in batches of {batch_size}...")

    for i in range(0, total, batch_size):
        chunk = docs[i:i+batch_size]
        payload = {"docs": chunk}

        r = requests.post(
            url,
            auth=(USERNAME, PASSWORD),
            headers={"Content-Type": "application/json"},
            data=json.dumps(payload),
        )
        
        if r.status_code not in (200, 201, 202):
            print(f"ERROR inserting batch {i}–{i+batch_size}: {r.text}")
            return
        
        print(f"Inserted batch {i}–{i+len(chunk)}")



def create_index(db_name, field, index_name):
    url = f"{COUCH_URL}/{db_name}/_index"
    payload = {
        "index": {"fields": [field]},
        "name": index_name,
        "type": "json",
    }
    r = requests.post(
        url,
        auth=(USERNAME, PASSWORD),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
    )
    print(f"Index '{index_name}' on {db_name}({field}) created."
          if r.status_code in (200, 201)
          else f"ERROR creating index on {db_name}: {r.text}")


def main():
    for filename, (db_name, key_field) in DATASETS.items():
        path = os.path.join(DATA_FOLDER, filename)

        if not os.path.exists(path):
            print(f"FILE NOT FOUND: {path}")
            continue

        # 1. Create DB
        create_db(db_name)

        # 2. Load CSV into documents
        docs = load_csv_to_docs(path, key_field)

        # 3. Insert in bulk
        bulk_insert(db_name, docs, batch_size=5000)

        # 4. Create index for benchmarking
        create_index(db_name, key_field, f"idx_{key_field}")


if __name__ == "__main__":
    main()
