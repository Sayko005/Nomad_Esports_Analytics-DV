import time
import random
import psycopg2

# ---- CONFIG ----
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "csgo"                
DB_USER = "postgres"               
DB_PASS = "admin"               
INTERVAL = 10                     
# ----------------

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                id SERIAL PRIMARY KEY,
                player_name VARCHAR(50),
                kills INT,
                deaths INT,
                score INT
            );
        """)
        conn.commit()

def simulate_activity(conn):
    with conn.cursor() as cur:
        action = random.choice(["insert", "update", "delete"])
        if action == "insert":
            name = random.choice(["s1mple", "ZywOo", "NiKo", "device", "m0NESY"])
            kills = random.randint(5, 30)
            deaths = random.randint(0, 15)
            score = kills * 10 - deaths * 5
            cur.execute(
                "INSERT INTO player_stats (player_name, kills, deaths, score) VALUES (%s, %s, %s, %s)",
                (name, kills, deaths, score)
            )
            print(f"[+] Inserted {name} (kills={kills}, deaths={deaths})")

        elif action == "update":
            cur.execute("SELECT id FROM player_stats ORDER BY RANDOM() LIMIT 1;")
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE player_stats SET kills = kills + %s WHERE id = %s;",
                    (random.randint(1, 5), row[0])
                )
                print(f"[*] Updated row id={row[0]}")

        elif action == "delete":
            cur.execute("SELECT id FROM player_stats ORDER BY RANDOM() LIMIT 1;")
            row = cur.fetchone()
            if row:
                cur.execute("DELETE FROM player_stats WHERE id = %s;", (row[0],))
                print(f"[-] Deleted row id={row[0]}")

        conn.commit()

def main():
    while True:
        try:
            with psycopg2.connect(
                host=DB_HOST, port=DB_PORT,
                dbname=DB_NAME, user=DB_USER, password=DB_PASS
            ) as conn:
                ensure_table(conn)
                simulate_activity(conn)
        except Exception as e:
            print("Database error:", e)
        time.sleep(INTERVAL)

if __name__ == "__main__":
    print("Starting CS:GO DB simulator...")
    main()
