# api.py

from fastapi import FastAPI
import sys
import os
import importlib.util
import redis
import json
import time

app = FastAPI()

# =======================
# FIX IMPORT PATH
# =======================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CGI_BIN = os.path.join(BASE_DIR, "cgi-bin")

# Add cgi-bin to path
sys.path.insert(0, CGI_BIN)

# Dynamically load db_connection.py
spec = importlib.util.spec_from_file_location("db_connection", os.path.join(CGI_BIN, "db_connection.py"))
db_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_module)

get_connection = db_module.get_connection


# =======================
# INVENTORY API
# =======================

@app.get("/inventory/{sku}")
def get_inventory(sku: str, location: str = None):
    conn = get_connection()
    cache = redis.Redis(host="localhost", port=6379, db=0)
    CACHE_KEY = "consolidated_sku"
    total_start_redis = time.time()
    if not conn:
        return {"error": "DB connection failed"}

    cur = conn.cursor()

    try:
        if location:
            # Use sku_info table
            cur.execute(
                """
                SELECT sku, location, on_hand 
                FROM sku_info 
                WHERE sku = %s AND location = %s
                """,
                (sku, location)
            )
            row = cur.fetchone()

            return (
                {"sku": row[0], "location": row[1], "on_hand": row[2]}
                if row else {"message": "No data found"}
            )

        else:

            # If hash exists ⇒ return from Redis
            if cache.exists(CACHE_KEY):
                print("📦 Redis cache hit")

                all_skus = cache.hgetall(CACHE_KEY)
                result = {k.decode(): int(v) for k, v in all_skus.items()}


                if sku == "all":
                    return {
                        "source": "redis",
                        "Total_Time": format_time(time.time() - total_start_redis),
                        "Total_records": len(result),
                        "data": result
                    }

                # Request is for a single SKU
                qty = result.get(sku)

                return {
                    "source": "redis",
                    "Total_Time": format_time(time.time() - total_start_redis),
                    # "Total_records": len(result),
                    "data": {"sku": sku, "qty": qty} if qty is not None else {"message": "No data found"}
                }

                # qty = result.get(sku)

                # if (sku == "all") 
                # # return {"source": "redis", "data": result}
                # return {
                #     "source": "redis",
                #     "Total_Time": format_time(time.time() - total_start_redis), 
                #     "data": {'sku': sku, 'qty': qty}
                    
                #     if qty is not None else {"message": "No data found" }}
    except Exception as e:
        return {"error": str(e)}

    finally:
        cur.close()
        conn.close()

def format_time(seconds):
    minutes = int(seconds // 60)
    sec = int(seconds % 60)
    ms = int((seconds - int(seconds)) * 1000)  # milliseconds
    return f"{minutes} min {sec} sec {ms} ms" if minutes else f"{sec} sec {ms} ms"