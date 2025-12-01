#!/usr/bin/env python3
print("Content-Type: text/plain\n")

import os
import time
import shutil
import ftplib
import csv
from datetime import datetime

from ftp_config import FTP_CONFIGS
from db_connection import get_connection


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
    except:
        return None


# ---------------------------------------------------------
# DATABASE IMPORT (MAIN LOGIC)
# ---------------------------------------------------------
def import_to_database(csv_file):
    try:
        conn = get_connection()
        cur = conn.cursor()

        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                sku = row.get("SKU", "").strip()
                qty = row.get("QTY", "").strip()

                if not sku:
                    continue

                cur.execute("""
                    INSERT INTO inventory_data (sku, qty, created_at)
                    VALUES (%s, %s, NOW())
                """, (sku, qty))

        conn.commit()
        cur.close()
        conn.close()

        print(f"DB Import OK → {csv_file}")

    except Exception as e:
        print(f"DB ERROR: {e}")


# ---------------------------------------------------------
# PROCESS FILE
# ---------------------------------------------------------
def process_file(file_path):
    print(f"Processing CSV → {file_path}")
    import_to_database(file_path)


# ---------------------------------------------------------
# FTP MODE
# ---------------------------------------------------------
def process_files_from_ftp(conf):
    ftp = connect_ftp(conf)
    if not ftp:
        return False

    local_folder = conf["local_read"]
    archive_folder = conf["local_archive"]

    ensure_folder(local_folder)
    ensure_folder(archive_folder)

    files = ftp.nlst()

    for file in files:
        if file in [".", ".."]:
            continue

        local_file = os.path.join(local_folder, file)
        remote_file = f"{conf['remote_incoming']}/{file}"

        # Download to local folder
        with open(local_file, "wb") as f:
            ftp.retrbinary(f"RETR {file}", f.write)

        # Import CSV into DB
        process_file(local_file)

        # Move remote file to FTP archive
        ftp.rename(remote_file, f"{conf['remote_archive']}/{file}")

        # Move local file to archive
        shutil.move(local_file, os.path.join(archive_folder, file))

    ftp.quit()
    return True


# ---------------------------------------------------------
# LOCAL MODE
# ---------------------------------------------------------
def process_files_from_local(conf):
    local_folder = conf["local_read"]
    archive_folder = conf["local_archive"]

    ensure_folder(local_folder)
    ensure_folder(archive_folder)

    for file in os.listdir(local_folder):
        if file in [".", ".."]:
            continue

        file_path = os.path.join(local_folder, file)

        if os.path.isdir(file_path):
            continue

        process_file(file_path)

        shutil.move(file_path, os.path.join(archive_folder, file))


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def run_import(ftp_name):
    conf = FTP_CONFIGS[ftp_name]
    log_folder = conf["local_log"]

    start = time.time()
    write_log(log_folder, f"START IMPORT → {ftp_name}")

    # Try FTP first
    ftp_ok = process_files_from_ftp(conf)

    # Fallback to local if FTP fails
    if not ftp_ok:
        write_log(log_folder, "FTP FAILED → Switching to LOCAL folder mode")
        process_files_from_local(conf)

    total = round(time.time() - start, 2)
    write_log(log_folder, f"FINISHED. Total time: {total} sec")

    print(f"Import completed in {total} sec")


# ---------------------------------------------------------
# EXECUTE
# ---------------------------------------------------------
if __name__ == "__main__":
    run_import("WESTSIDE_1")
