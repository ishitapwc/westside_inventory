#!/usr/bin/env python3
print("Content-Type: text/html\n")

import os
import csv
import shutil
import time
from datetime import datetime
from db_connection import get_connection
from psycopg2.extras import execute_values
import redis

CSV_FOLDER = "/var/www/html/westside/csv"
ARCHIVE_FOLDER = "/var/www/html/westside/archive"
BATCH_SIZE = 5000  


# Format time
def format_time(seconds):
    minutes = int(seconds // 60)
    sec = int(seconds % 60)
    ms = int((seconds - int(seconds)) * 1000)  # milliseconds
    return f"{minutes} min {sec} sec {ms} ms" if minutes else f"{sec} sec {ms} ms"


# Bulk Insert/Update (overwrite, handle duplicates in batch)
def process_batch(cur, batch):
    
    unique_rows = {}
    for sku, location, on_hand, updated_at in batch:
        unique_rows[(sku, location)] = (sku, location, on_hand, updated_at)

    values = list(unique_rows.values())

    query = """
        INSERT INTO sku_info (sku, location, on_hand, updated_at)
        VALUES %s
        ON CONFLICT (sku, location)
        DO UPDATE SET
            on_hand = EXCLUDED.on_hand,
            updated_at = EXCLUDED.updated_at;
    """
    execute_values(cur, query, values)


# Insert or update file
def insert_or_update(file_path):
    conn = get_connection()
    if not conn:
        print(" DB connection failed<br>")
        return False, 0

    cur = conn.cursor()
    start_time = time.time()
    batch = []
    error_found = False

    try:
        with open(file_path, "r") as file:
            reader = csv.reader(file)
            next(reader, None)  # skip header

            for idx, row in enumerate(reader, start=2):
                try:
                    sku = row[0].strip()
                    location = row[1].strip()
                    on_hand = int(float(row[2].strip()))
                    updated_at = datetime.now()

                    batch.append((sku, location, on_hand, updated_at))

                    if len(batch) >= BATCH_SIZE:
                        process_batch(cur, batch)
                        batch.clear()

                except Exception as row_err:
                    print(f" Row {idx} ERROR: {row_err}<br>")
                    error_found = True

            
            if batch:
                process_batch(cur, batch)

        if error_found:
            conn.rollback()
            return False, 0

        conn.commit()
        return True, time.time() - start_time

    except Exception as e:
        print(f" File Processing Error: {e}<br>")
        conn.rollback()
        return False, 0

    finally:
        cur.close()
        conn.close()


# Process all CSV files
def process_all_csv():
    total_start = time.time()

    files = [f for f in os.listdir(CSV_FOLDER) if f.endswith(".csv")]
    if not files:
        print(" No CSV files found.<br>")
        return

    for file in files:
        print(f"<br> Processing: {file}<br>")
        path = os.path.join(CSV_FOLDER, file)

        success, duration = insert_or_update(path)

        if success:
            shutil.move(path, os.path.join(ARCHIVE_FOLDER, file))
            print(f" Moved {file} to archive folder <br>")
            print(f" Time: {format_time(duration)}<br>")
        else:
            print(f" FAILED: {file} to NOT MOVED<br>")

    print("<br>============================<br>")
    print(f" TOTAL Time: {format_time(time.time() - total_start)}<br>")
    print("============================<br>")

    # Run stored procedure after all CSV files are processed
    

process_all_csv()

# Call stored procedure consolidate_sku_totals()
def call_consolidate_sku():
    total_start_consolidate = time.time()
    conn = get_connection()
    # procedure_name = "consolidate_sku_totals"
    if not conn:
        print(" DB connection failed for consolidate_sku_totals()<br>")
        return False

    try:
        cur = conn.cursor()

        # query = """
        # SELECT routine_name
        # FROM information_schema.routines
        # WHERE routine_type = 'PROCEDURE'
        # AND routine_name = %s;
    # """

        # Call PostgreSQL stored procedure
        cur.execute("CALL consolidate_sku_totals();")

        conn.commit()
        print(" Consolidated SKU totals updated.<br>")
        
        print("<br>============================<br>")
        print(f" TOTAL Time: {format_time(time.time() - total_start_consolidate)}<br>")
        print("============================<br>")

        return True

    except Exception as e:
        print(f" Error calling consolidate_sku_totals(): {e}<br>")
        conn.rollback()
        return False

    finally:
        cur.close()
        conn.close()

call_consolidate_sku()

# def stored_redis_cache():
#     # print(" DB connection failed<br>")
#     # return False
#     try:
#         total_start_redis = time.time()
#         cache = redis.Redis(host="localhost", port=6379, db=0)
#         conn = get_connection()
#         if not conn:
#             print(" DB connection failed<br>")
#             return False, 0

#             cur = conn.cursor()
#             cur.execute("SELECT sku, qty FROM consolidated_sku")
#             rows = cur.fetchall()

#             pipeline = cache.pipeline()   # super fast batch write

#             for sku, qty in rows:
#                 pipeline.hset('consolidated_sku_datas', sku, qty)

#             pipeline.execute()  # writes all at once (fast)

#             # converting output
#             # result = {r[0]: r[1] for r in rows}

#             print("Stored data Redis cache<br>")
            
#             print("<br>============================<br>")
#             print(f" TOTAL Time: {format_time(time.time() - total_start_redis)}<br>")
#             print("============================<br>")
#             return True

#             # return {"source": "database", "data": result}
#     except Exception as e:
#         print(f" Error clearing Redis cache: {e}<br>")
#         return False
# stored_redis_cache()


def stored_redis_cache():
    try:
        total_start_redis = time.time()
        cache = redis.Redis(host="localhost", port=6379, db=0)

        conn = get_connection()
        if not conn:
            print("DB connection failed<br>")
            return False, 0

        cur = conn.cursor()
        cur.execute("SELECT sku, qty FROM consolidated_sku")
        rows = cur.fetchall()

        pipeline = cache.pipeline()   # super fast batch write

        for sku, qty in rows:
            pipeline.hset("consolidated_sku", sku, qty)

        pipeline.execute()  # writes all at once

        print("Stored data in Redis cache<br>")
        print("<br>============================<br>")
        print(f" TOTAL Time: {format_time(time.time() - total_start_redis)}<br>")
        print("============================<br>")

        return True

    except Exception as e:
        print(f"Error storing Redis cache: {e}<br>")
        return False

stored_redis_cache()

# print("<br>CSV Import Completed.<br>")


