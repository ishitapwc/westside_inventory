#!/usr/bin/env python3
from fastapi import FastAPI
import os
import sys
import importlib.util
import json
import requests
from datetime import datetime
import time
from psycopg2.extras import execute_values

# =======================
# FIX IMPORT PATH
# =======================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CGI_BIN = os.path.join(BASE_DIR, "cgi-bin")
sys.path.insert(0, CGI_BIN)

# Load db_connection.py dynamically
spec = importlib.util.spec_from_file_location("db_connection", os.path.join(CGI_BIN, "db_connection.py"))
db_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_module)
get_connection = db_module.get_connection

# =======================
# CONFIG
# =======================
API_URL = "https://intapppreprod2.tataunistore.com/PartnerConnect/PC_100001/AP_INVENTORY/AV_2.0/AVT_JSON_INVEN"
API_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiI2OTA5MTAiLCJleHAiOjE3NjQxNDk5MjcsImlhdCI6MTc2NDA2MzUyN30.ELNtRkEYKWNNT46EZ5HvvSgyMAZoOdRAaVaLnragrMMStCdDdZkvdKnh6urolKG4KfgDUzVDF7LzvHub-7YMFQ"  # Replace with your token
SAFETY_STOCK_DEFAULT = 10

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_TOKEN}"
}

# =======================
# FASTAPI APP
# =======================
app = FastAPI(title="Inventory API")


@app.get("/send_inventory/")
def send_inventory():

    start_time = time.time()

    conn = get_connection()
    if not conn:
        return {"error": "DB connection failed"}

    cursor = conn.cursor()
    cursor.execute("SELECT sku, location, avb_qty FROM consolidated_store_sku WHERE is_send = 0")
    rows = cursor.fetchall()

    if not rows:
        return {"message": "No inventory to send"}

    # Prepare payload
    payload_items = []
    ids_to_update = []

    for r in rows:
        payload_items.append({
            "SKU": r[0],
            "SlaveID": r[1],
            "Inventory": r[2],
            "SafetyStock": SAFETY_STOCK_DEFAULT
        })
        ids_to_update.append((r[0], r[1]))

    payload = {"inventoryUpdate": {"item": payload_items}}

    # ========= PRINT PAYLOAD IN BROWSER =========
    # Print payload
    print(json.dumps(payload, indent=4))

    # ========= SEND API (uncomment to enable) =========
    # """
    # try:
    #     response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
    #     print(f"Status Code: {response.status_code}")
    #     print(f"Response: {response.text}")
    # except Exception as e:
    #     return {"error": str(e)}
    # """

    # ========= BATCH UPDATE USING TEMP TABLE (VERY FAST) =========
    try:
        # Step 1: Create temp table
        cursor.execute("""
            CREATE TEMP TABLE temp_update (
                sku TEXT,
                location TEXT
            ) ON COMMIT DROP;
        """)

        # Step 2: Insert all values into temp table
        execute_values(cursor,
            "INSERT INTO temp_update (sku, location) VALUES %s",
            ids_to_update
        )

        # Step 3: Update using JOIN
        cursor.execute("""
            UPDATE consolidated_store_sku t
            SET is_send = 1
            FROM temp_update u
            WHERE t.sku = u.sku AND t.location = u.location;
        """)

        conn.commit()

    except Exception as e:
        conn.rollback()
        return {"error": f"Update failed: {str(e)}"}

    finally:
        cursor.close()
        conn.close()

    # ========= PROCESS TIME =========
    process_time = round(time.time() - start_time, 2)

    return {
        "message": f"{len(payload_items)} records processed",
        "process_time_seconds": process_time
    }
