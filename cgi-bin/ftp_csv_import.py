#!/usr/bin/env python3
print("Content-Type: text/plain\n")

import os
import time
import shutil
import ftplib
import csv
from datetime import datetime
from psycopg2.extras import execute_values
import redis

from ftp_config import FTP_CONFIGS
from db_connection import get_connection

BATCH_SIZE = 5000  # bulk insert batch size

# ---------------------------------------------------------
# Create folder if not exists
# ---------------------------------------------------------
def ensure_folder(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

# ---------------------------------------------------------
# Log Writer
# ---------------------------------------------------------
def write_log(log_folder, message):
    ensure_folder(log_folder)
    log_file = os.path.join(log_folder, f"log_{datetime.now().strftime('%Y_%m_%d')}.txt")
    with open(log_file, "a") as f:
        f.write(f"{datetime.now()} - {message}\n")

# ---------------------------------------------------------
# FTP Connect
# ---------------------------------------------------------
def connect_ftp(conf):
    try:
        ftp = ftplib.FTP(conf["host"], conf["user"], conf["pass"])
        ftp.cwd(conf["remote_incoming"])
        return ftp
    except Exception as e:
        return None

# ---------------------------------------------------------
# Format time for logs
# ---------------------------------------------------------
def format_time(seconds):
    minutes = int(seconds // 60)
    sec = int(seconds % 60)
    ms = int((seconds - int(seconds)) * 1000)
    return f"{minutes} min {sec} sec {ms} ms" if minutes else f"{sec} sec {ms} ms"

# ---------------------------------------------------------
# BULK UPSERT CSV into DB with batch timing
# ---------------------------------------------------------
def process_batch(cur, batch, log_folder=None, file_name=None, batch_number=None):
    batch_start = time.time()
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
    batch_duration = time.time() - batch_start

    if log_folder and file_name:
        batch_info = f"Batch {batch_number}" if batch_number else ""
        write_log(log_folder, f"{batch_info} for {file_name} → {len(batch)} rows inserted/updated in {format_time(batch_duration)}")

# ---------------------------------------------------------
# Insert or update file
# ---------------------------------------------------------
def insert_or_update(file_path, log_folder=None):
    conn = get_connection()
    if not conn:
        print(" DB connection failed<br>")
        return False, 0

    cur = conn.cursor()
    start_time = time.time()
    batch = []
    error_found = False
    batch_number = 0

    try:
        with open(file_path, "r", encoding="utf-8") as file:
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
                        batch_number += 1
                        process_batch(cur, batch, log_folder, os.path.basename(file_path), batch_number)
                        batch.clear()

                except Exception as row_err:
                    print(f" Row {idx} ERROR: {row_err}<br>")
                    error_found = True

            if batch:
                batch_number += 1
                process_batch(cur, batch, log_folder, os.path.basename(file_path), batch_number)

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

# ---------------------------------------------------------
# Process single file
# ---------------------------------------------------------
def process_file(file_path, log_folder=None):
    print(f"Processing CSV → {file_path}")
    return insert_or_update(file_path, log_folder)

# ---------------------------------------------------------
# FTP Mode
# ---------------------------------------------------------
def process_files_from_ftp(conf):
    ftp_start = time.time()
    ftp = connect_ftp(conf)
    ftp_duration = time.time() - ftp_start

    if not ftp:
        return False

    local_folder = conf["local_read"]
    archive_folder = conf["local_archive"]
    log_folder = conf["local_log"]

    ensure_folder(local_folder)
    ensure_folder(archive_folder)

    files = ftp.nlst()
    for file in files:
        if file in [".", ".."]:
            continue

        local_file = os.path.join(local_folder, file)
        remote_file = f"{conf['remote_incoming']}/{file}"

        # Download file
        download_start = time.time()
        with open(local_file, "wb") as f:
            ftp.retrbinary(f"RETR {file}", f.write)
        download_time = time.time() - download_start
        write_log(log_folder, f"Downloaded {file} in {format_time(download_time)}")

        # Process CSV
        success, duration = process_file(local_file, log_folder)
        if success:
            print(f" DB Import OK → {file}, Time: {format_time(duration)}")
        else:
            print(f" DB Import FAILED → {file}")

        # Move remote file to FTP archive
        ftp_archive_start = time.time()
        ftp.rename(remote_file, f"{conf['remote_archive']}/{file}")
        ftp_archive_time = time.time() - ftp_archive_start
        write_log(log_folder, f"FTP archive move {file} in {format_time(ftp_archive_time)}")

        # Move local file to archive
        local_archive_start = time.time()
        shutil.move(local_file, os.path.join(archive_folder, file))
        local_archive_time = time.time() - local_archive_start
        write_log(log_folder, f"Local archive move {file} in {format_time(local_archive_time)}")

    ftp.quit()
    write_log(log_folder, f"FTP processing completed in {format_time(time.time() - ftp_start)}")
    return True

# ---------------------------------------------------------
# Local Mode (fallback)
# ---------------------------------------------------------
def process_files_from_local(conf):
    local_folder = conf["local_read"]
    archive_folder = conf["local_archive"]
    log_folder = conf["local_log"]

    ensure_folder(local_folder)
    ensure_folder(archive_folder)

    files = [f for f in os.listdir(local_folder) if os.path.isfile(os.path.join(local_folder, f))]
    for file in files:
        local_file = os.path.join(local_folder, file)

        # Process CSV
        success, duration = process_file(local_file, log_folder)
        if success:
            print(f" DB Import OK → {file}, Time: {format_time(duration)}")
            # Move local file to archive
            move_start = time.time()
            shutil.move(local_file, os.path.join(archive_folder, file))
            move_time = time.time() - move_start
            write_log(log_folder, f"Local archive move {file} in {format_time(move_time)}")
        else:
            print(f" DB Import FAILED → {file}")

# ---------------------------------------------------------
# Stored Procedure
# ---------------------------------------------------------
def call_consolidate_sku(log_folder=None):
    total_start = time.time()
    conn = get_connection()
    if not conn:
        print(" DB connection failed for consolidate_sku_totals()<br>")
        return False

    try:
        cur = conn.cursor()
        cur.execute("CALL consolidate_sku_totals();")
        conn.commit()
        proc_time = format_time(time.time() - total_start)
        print(f"Consolidated SKU totals updated in {proc_time}")
        if log_folder:
            write_log(log_folder, f"Stored procedure consolidate_sku_totals executed in {proc_time}")
        return True
    except Exception as e:
        print(f" Error calling consolidate_sku_totals(): {e}<br>")
        return False
    finally:
        cur.close()
        conn.close()

# ---------------------------------------------------------
# Redis Cache
# ---------------------------------------------------------
def stored_redis_cache(log_folder=None):
    try:
        start_time = time.time()
        cache = redis.Redis(host="localhost", port=6379, db=0)
        conn = get_connection()
        if not conn:
            print(" DB connection failed for Redis<br>")
            return False, 0

        cur = conn.cursor()
        cur.execute("SELECT sku, qty FROM consolidated_sku")
        rows = cur.fetchall()

        pipeline = cache.pipeline()
        for sku, qty in rows:
            pipeline.hset("consolidated_sku", sku, qty)
        pipeline.execute()

        duration = format_time(time.time() - start_time)
        print(f"Stored data in Redis cache in {duration}")
        if log_folder:
            write_log(log_folder, f"Redis cache updated in {duration}")
        return True
    except Exception as e:
        print(f" Error storing Redis cache: {e}<br>")
        return False
    finally:
        cur.close()
        conn.close()

# ---------------------------------------------------------
# MAIN PROCESS 
# ---------------------------------------------------------
def run_import(ftp_name):
    conf = FTP_CONFIGS[ftp_name]
    log_folder = conf["local_log"]

    start_time = time.time()
    write_log(log_folder, f"START IMPORT → {ftp_name}")

    ftp_ok = process_files_from_ftp(conf)
    if not ftp_ok:
        write_log(log_folder, "FTP FAILED → Using LOCAL folder")
        process_files_from_local(conf)

    # Call stored procedure
    call_consolidate_sku(log_folder)

    # Update Redis cache
    stored_redis_cache(log_folder)

    total = round(time.time() - start_time, 2)
    write_log(log_folder, f"FINISHED IMPORT → Total time: {total} sec")
    print(f"Import completed in {total} sec")

# ---------------------------------------------------------
# EXECUTE
# ---------------------------------------------------------
if __name__ == "__main__":
    run_import("WESTSIDE_1")
