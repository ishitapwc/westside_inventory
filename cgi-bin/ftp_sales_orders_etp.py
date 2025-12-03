#!/usr/bin/env python3
print("Content-Type: text/html\n")

import os
import shutil
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import execute_values
from openpyxl import load_workbook
from db_connection import get_connection
import csv
import redis

import ftplib
import logging
import time
from ftp_config import FTP_CONFIGS


# =======================================================
# FTP CONFIG
# =======================================================
CFG = FTP_CONFIGS["WESTSIDE_ETP"]
FTPDIR = FTP_CONFIGS["GLOBAL"]

FTP_HOST = CFG["host"]
FTP_USER = CFG["user"]
FTP_PASS = CFG["pass"]

REMOTE_INCOMING = CFG["remote_incoming"]
REMOTE_ARCHIVE = CFG["remote_archive"]

INPUT_FOLDER = FTPDIR["etp_local_read"]
ARCHIVE_FOLDER = FTPDIR["etp_local_archive"]
LOG_DIR = FTPDIR["etp_local_log"]

BATCH_SIZE = 5000

# Create necessary directories if not exist
os.makedirs(INPUT_FOLDER, exist_ok=True)
os.makedirs(ARCHIVE_FOLDER, exist_ok=True)

# =======================================================
# Time Calculation
# =======================================================
def format_duration(seconds):
    mins = int(seconds // 60)
    secs = int(seconds % 60)
    ms = int((seconds - int(seconds)) * 1000)
    return f"{mins} min {secs} sec {ms} ms"

# =======================================================
# LOGGING SETUP
# =======================================================
os.makedirs(LOG_DIR, exist_ok=True)

today = time.strftime("%Y-%m-%d")
LOG_FILE = os.path.join(LOG_DIR, f"sales_order_import_{today}.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logging.info("---------------WESTSIDE_ETP SALES ORDER IMPORT STARTED ---------------")


# =======================================================
# FTP DOWNLOAD 
# =======================================================
def ftp_download_files():
    logging.info("[FTP] Connecting to FTP Server...")

    try:
        ftp = ftplib.FTP()
        ftp.connect(FTP_HOST, 21, timeout=60)
        ftp.login(FTP_USER, FTP_PASS)
    except Exception as e:
        logging.error(f"[FTP] Connect/login failed: {e}")
        return []

    try:
        ftp.cwd(REMOTE_INCOMING)
        files = ftp.nlst()
    except Exception as e:
        logging.error(f"[FTP] Unable to list files: {e}")
        ftp.quit()
        return []

    downloaded = []
    for f in files:
        if not (f.lower().endswith(".csv")):
            continue

        local_path = os.path.join(INPUT_FOLDER, f)
        os.makedirs(INPUT_FOLDER, exist_ok=True)

        try:
            with open(local_path, "wb") as fp:
                ftp.retrbinary("RETR " + f, fp.write)
            ftp.rename(f"{REMOTE_INCOMING}/{f}", f"{REMOTE_ARCHIVE}/{f}")
            downloaded.append(f)
        except Exception as e:
            logging.error(f"[FTP ERROR] {f}: {e}")

    ftp.quit()
    return downloaded


# =======================================================
# BATCH INSERT WRAPPER
# =======================================================
def insert_batch_and_count(cursor, insert_query, batch_data):
    if not batch_data:
        return 0
    n = len(batch_data)
    execute_values(cursor, insert_query, batch_data)
    batch_data.clear()
    return n


# =======================================================
# PROCESS CSV 
# =======================================================
def process_csv(file_path, cursor):
    logging.info(f"[CSV] Processing: {file_path}")

    batch_data = []
    total = 0

    query = """
        INSERT INTO sales_orders (sku, sale_qty, store_code, consolidate, is_read)
        VALUES %s
    """

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            sku = row.get("itemnumber", "").strip()
            store_code = row.get("StoreCode", "").strip()
            qty = row.get("Sum Of Qty Sold", "").strip()

            if not sku:
                continue

            try:
                qty = int(float(qty)) if qty else 0
            except:
                qty = 0

            batch_data.append((sku, qty, store_code, 0, 0))

            if len(batch_data) >= BATCH_SIZE:
                total += insert_batch_and_count(cursor, query, batch_data)

    if batch_data:
        total += insert_batch_and_count(cursor, query, batch_data)

    logging.info(f"[CSV] Total inserted: {total}")
    return total


# =======================================================
# MAIN PROCESS
# =======================================================
def orderProcess():
    start_time = time.time()

    try:
        ftp_download_files()
    except:
        pass

    conn = get_connection()
    if not conn:
        print("DB connection failed<br>")
        return

    cursor = conn.cursor()

    files = [
        f for f in os.listdir(INPUT_FOLDER)
        if f.lower().endswith((".csv"))
    ]

    if not files:
        print("No sales order files found<br>")
        return

    for f in files:
        path = os.path.join(INPUT_FOLDER, f)
        print(f"Processing File: {f} <br>")

        try:
            if f.lower().endswith(".csv"):
                count = process_csv(path, cursor)

            conn.commit()
            print(f"Inserted rows: {count} <br>")

            shutil.move(path, os.path.join(ARCHIVE_FOLDER, f))
            print(f"Archived: {f} <br>")

        except Exception as e:
            conn.rollback()
            print(f"Error in {f}: {e} <br>")

    # Run Stored Procedure
    try:
        cursor.execute("CALL process_sales_orders()")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Procedure error: {e}")

    cursor.close()
    conn.close()
    print("All files processed<br>")

    # ADD END TIMER + LOG
    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"[TIME] Total Duration for {type}: {format_duration(duration)}")

# ---------------------------------------------------------
# Redis Cache
# ---------------------------------------------------------
def stored_redis_cache():
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

        # ADD END TIMER + LOG
        end_time = time.time()
        duration = end_time - start_time
        logging.info(f"Stored data in Redis cache Total Duration for : {format_duration(duration)}")
        print(f"Stored data in Redis cache in {format_duration(duration)}")

        return True
    except Exception as e:
        print(f" Error storing Redis cache: {e}<br>")
        return False
    finally:
        cur.close()
        conn.close()

orderProcess()
stored_redis_cache()

