#!/usr/bin/env python3
import cgi
import cgitb
cgitb.enable()

import csv
import os
import time
from psycopg2.extras import execute_values
from db_connection import get_connection

print("Content-Type: text/plain\n")

# ----------------------------------------------------
# FOLDERS
# ----------------------------------------------------
BASE_DIR = "/var/www/html/python/westside_inventory"
CSV_FOLDER = f"{BASE_DIR}/tul_csv"
ARCHIVE_FOLDER = f"{BASE_DIR}/tul_csv_archive"

os.makedirs(ARCHIVE_FOLDER, exist_ok=True)


# ----------------------------------------------------
# SKU NORMALIZATION (Fix scientific notation)
# ----------------------------------------------------
def normalize_sku(sku):
    try:
        sku = str(sku).strip()
        if sku == "" or sku.upper() in ["#N/A", "N/A", "NA"]:
            return ""

        # scientific notation → convert to full number
        if "E" in sku.upper():
            return str(int(float(sku)))

        return sku
    except:
        return ""


# ----------------------------------------------------
# PROCESS ONE CSV FILE
# ----------------------------------------------------
def process_csv_file(filename):

    filepath = os.path.join(CSV_FOLDER, filename)
    conn = get_connection()
    cur = conn.cursor()

    start_time = time.time()

    # Load existing sku+location to avoid duplicates
    cur.execute("SELECT sku, location FROM tul_test_sku")
    existing = {(r[0], r[1]) for r in cur.fetchall()}

    batch_data = []
    rows_inserted = 0

    with open(filepath, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            # Read fields
            raw_sku = row.get("SKU available in TUL file", "")
            sku = normalize_sku(raw_sku)
            location = row.get("location", "").strip()
            qty_raw = row.get("on_hand", "").strip()

            # Validate SKU only
            if not sku:
                continue

            if not location:
                continue

            # Convert qty
            try:
                qty = int(float(qty_raw)) if qty_raw not in ["", "#N/A", "N/A"] else 0
            except:
                qty = 0

            # Skip duplicates
            if (sku, location) in existing:
                continue

            batch_data.append((sku, location, qty))
            existing.add((sku, location))  # add to memory to avoid inserting again

            # Insert in chunks
            if len(batch_data) >= 5000:
                execute_values(cur,
                    "INSERT INTO tul_test_sku (sku, location, qty) VALUES %s",
                    batch_data
                )
                rows_inserted += len(batch_data)
                batch_data.clear()

    # Insert last remaining records
    if batch_data:
        execute_values(cur,
            "INSERT INTO tul_test_sku (sku, location, qty) VALUES %s",
            batch_data
        )
        rows_inserted += len(batch_data)

    conn.commit()
    cur.close()
    conn.close()

    # Move file to archive
    os.rename(filepath, os.path.join(ARCHIVE_FOLDER, filename))

    process_time = round(time.time() - start_time, 2)
    return rows_inserted, process_time


# ----------------------------------------------------
# MAIN
# ----------------------------------------------------
def main():

    files = [f for f in os.listdir(CSV_FOLDER) if f.endswith(".csv")]

    if not files:
        print("No CSV files found in folder.")
        return

    total_rows = 0
    total_time = 0

    print("Processing started...\n")

    for file in files:
        rows, t = process_csv_file(file)
        total_rows += rows
        total_time += t
        print(f"File: {file} → Rows Inserted: {rows} | Time: {t} sec")

    print("\n-------------------------------")
    print(f"TOTAL ROWS INSERTED: {total_rows}")
    print(f"TOTAL TIME TAKEN: {round(total_time, 2)} sec")
    print("-------------------------------")


if __name__ == "__main__":
    main()
