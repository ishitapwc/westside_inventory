#!/usr/bin/env python3

import csv
import time
from datetime import datetime
from io import StringIO
from ftplib import FTP
from psycopg2.extras import execute_values
from db_connection import get_connection
import os

# ----------------------------
# Configuration
# ----------------------------
FTP_HOST = "ftp.example.com"           # ← replace with real FTP host
FTP_USER = "username@example.com"      # ← replace with real FTP username
FTP_PASS = "YourSecurePassword123!"    # ← replace with real FTP password
FTP_FOLDER = "/csv"
FTP_ARCHIVE_FOLDER = "/archive"
BATCH_SIZE = 5000

# Log file
LOG_FILE = "/var/www/html/westside/log/ftp_csv_process.log"

# Ensure log folder exists
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# ----------------------------
# Logging helper
# ----------------------------
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"[{timestamp}] {msg}\n")
    print(f"[{timestamp}] {msg}")  # Optional: also print to console

# ----------------------------
# Helper functions
# ----------------------------
def format_time(seconds):
    minutes = int(seconds // 60)
    sec = int(seconds % 60)
    ms = int((seconds - int(seconds)) * 1000)
    return f"{minutes} min {sec} sec {ms} ms" if minutes else f"{sec} sec {ms} ms"

def process_batch(cur, batch):
    """Insert or update batch of rows in the database."""
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

# ----------------------------
# Process CSV from FTP
# ----------------------------
def insert_or_update_ftp(file_name, ftp_conn):
    """Fetch CSV from FTP and insert/update into database."""
    csv_data = StringIO()
    ftp_conn.retrlines(f"RETR {file_name}", lambda line: csv_data.write(line + "\n"))
    csv_data.seek(0)

    conn = get_connection()
    if not conn:
        log("DB connection failed")
        return False, 0

    cur = conn.cursor()
    batch = []
    error_found = False
    start_time = time.time()

    try:
        reader = csv.reader(csv_data)
        next(reader, None)  # skip header

        for idx, row in enumerate(reader, start=2):
            # Skip empty or incomplete rows
            if not row or len(row) < 3:
                log(f" Row {idx} skipped (empty or incomplete)")
                continue

            # Skip rows where first 3 columns are blank or just "."
            if all(cell.strip() in ("", ".") for cell in row[:3]):
                log(f" Row {idx} skipped (invalid data)")
                continue

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
                log(f" Row {idx} ERROR: {row_err}")
                error_found = True

        if batch:
            process_batch(cur, batch)

        if error_found:
            conn.rollback()
            return False, 0

        conn.commit()
        return True, time.time() - start_time

    except Exception as e:
        log(f" File Processing Error: {e}")
        conn.rollback()
        return False, 0

    finally:
        cur.close()
        conn.close()

# ----------------------------
# Move processed file to FTP archive
# ----------------------------
def move_ftp_file_to_archive(ftp_conn, file_name):
    """Move processed file to FTP archive folder."""
    try:
        # Ensure archive folder exists
        try:
            ftp_conn.mkd(FTP_ARCHIVE_FOLDER)
        except Exception:
            pass  # Folder already exists

        # Move the file
        ftp_conn.rename(f"{FTP_FOLDER}/{file_name}", f"{FTP_ARCHIVE_FOLDER}/{file_name}")
        log(f"Moved {file_name} to archive folder on FTP")
    except Exception as e:
        log(f"Failed to move {file_name} to archive: {e}")

# ----------------------------
# Process all files from FTP
# ----------------------------
def process_all_ftp_csv():
    ftp = FTP(FTP_HOST)
    ftp.login(user=FTP_USER, passwd=FTP_PASS)
    ftp.cwd(FTP_FOLDER)

    files = ftp.nlst()
    # Skip invalid entries: blank, ".", ".." and non-CSV files
    # files = [f for f in files if f and f not in (".", "..")]
    # Filter out invalid entries
    files = [f for f in files if f and f not in (".", "..")]
    
    if not files:
        # log("No valid CSV files found on FTP")
        ftp.quit()
        return

    total_start = time.time()

    for file in files:
        log(f"Processing: {file}")
        success, duration = insert_or_update_ftp(file, ftp)
        if success:
            log(f"Processed {file} successfully. Time: {format_time(duration)}")
            move_ftp_file_to_archive(ftp, file)
        else:
            log(f"FAILED: {file}")

    log(f"Total Time: {format_time(time.time() - total_start)}")
    ftp.quit()

# ----------------------------
# Run the process
# ----------------------------
if __name__ == "__main__":
    process_all_ftp_csv()
