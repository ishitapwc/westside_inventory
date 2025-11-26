#!/usr/bin/env python3

import psycopg2

print("Content-Type: text/html\n")

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname="westside",
        user="root",
        password="Sushil@123456",
        host="localhost",
        port="5432"
    )

    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()

    # Print output in browser
    print("<h1>PostgreSQL Connection Successful!</h1>")
    print("<p>Database Version:</p>")
    print(f"<pre>{version}</pre>")

    # Query all DB names
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
    databases = cur.fetchall()

    print("<h1>PostgreSQL Databases</h1>")
    print("<ul>")
    for db in databases:
        print(f"<li>{db[0]}</li>")
    print("</ul>")

except Exception as e:
    print("<h1>Error connecting to PostgreSQL</h1>")
    print(f"<pre>{e}</pre>")


    


