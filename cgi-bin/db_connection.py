# db_connection.py

import psycopg2
import db_config

def get_connection():
    try:
        conn = psycopg2.connect(
            host=db_config.DB_HOST,
            port=db_config.DB_PORT,
            dbname=db_config.DB_NAME,
            user=db_config.DB_USER,
            password=db_config.DB_PASSWORD
        )
        return conn
    except Exception as e:
        print("Error connecting to database:", e)
        return None
