import psycopg2

# Same config as the app
DB_CONFIG = {
    "dbname": "fraud_detection_db",
    "user": "admin",
    "password": "password",
    "host": "127.0.0.1", # We will test 127.0.0.1 if this fails
    "port": "5432"
}

print(f"Attempting to connect to: {DB_CONFIG['host']}:{DB_CONFIG['port']}...")

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Connection Successful!")
    
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM fraud_alerts;")
        count = cur.fetchone()[0]
        print(f"📊 Total Rows in Table: {count}")
        
        cur.execute("SELECT * FROM fraud_alerts ORDER BY timestamp DESC LIMIT 5;")
        rows = cur.fetchall()
        print("📝 Latest 5 rows:")
        for row in rows:
            print(row)
            
    conn.close()

except Exception as e:
    print("\n❌ CONNECTION FAILED")
    print(f"Error Type: {type(e).__name__}")
    print(f"Error Message: {e}")