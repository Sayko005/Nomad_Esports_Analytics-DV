# assn3_live_2025_furia.py
import os, random, time
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, text

DB_URL   = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:admin@localhost:5432/csgo")
EVENT_ID = 9999
YEAR     = 2025
MAPS     = ["Mirage","Inferno","Nuke","Overpass","Ancient","Vertigo","Anubis"]
FURIA    = "FURIA"

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

def ensure_event(conn):
    conn.execute(text("""
        INSERT INTO events (event_id, event_name)
        VALUES (:eid, :name)
        ON CONFLICT (event_id) DO NOTHING
    """), {"eid": EVENT_ID, "name": "Live Demo 2025"})

def teams_pool(conn):
    rows = conn.execute(text("""
        SELECT team FROM (
            SELECT DISTINCT team_1 AS team FROM matches
            UNION ALL
            SELECT DISTINCT team_2 FROM matches
            UNION ALL
            SELECT DISTINCT team FROM players_stage
            UNION ALL
            SELECT DISTINCT team FROM players_raw
        ) t
        WHERE team IS NOT NULL AND team <> ''
        LIMIT 500
    """)).fetchall()
    pool = sorted({r[0] for r in rows if r[0]})
    # гарантируем FURIA в пуле
    if FURIA not in pool: pool.append(FURIA)
    return pool

def next_match_id(conn):
    return int(conn.execute(text("SELECT COALESCE(MAX(match_id),0)+1 FROM matches")).scalar())

def rand_date_2025():
    start = date(YEAR, 1, 1)
    end   = min(date(YEAR, 12, 31), datetime.utcnow().date())
    dlt   = (end - start).days
    return start + timedelta(days=random.randint(0, max(0, dlt)))

def insert_one(conn, pool):
    # одна команда всегда FURIA
    other = random.choice([t for t in pool if t != FURIA])
    t1, t2 = (FURIA, other) if random.random() < 0.5 else (other, FURIA)
    mid    = next_match_id(conn)
    mdate  = rand_date_2025()
    m      = random.choice(MAPS)
    r1, r2 = random.randint(6, 19), random.randint(6, 19)
    while r1 == r2: r2 = random.randint(6, 19)
    winner = 1 if r1 > r2 else 2

    conn.execute(text("""
        INSERT INTO matches (match_id, event_id, team_1, team_2, match_date)
        VALUES (:mid, :eid, :t1, :t2, :d)
        ON CONFLICT (match_id) DO NOTHING
    """), {"mid": mid, "eid": EVENT_ID, "t1": t1, "t2": t2, "d": mdate})

    conn.execute(text("""
        INSERT INTO results (match_id, event_id, map, result_1, result_2, match_winner)
        VALUES (:mid, :eid, :map, :r1, :r2, :w)
        ON CONFLICT DO NOTHING
    """), {"mid": mid, "eid": EVENT_ID, "map": m, "r1": r1, "r2": r2, "w": winner})

    return mid, mdate, t1, t2, m, r1, r2

def main(count=30, delay=0):
    with engine.begin() as conn:
        ensure_event(conn)
        pool = teams_pool(conn)
    print(f"[run] DB={DB_URL} event_id={EVENT_ID} year={YEAR} pool={len(pool)} (FURIA enforced)")

    i = 0
    while count == 0 or i < count:
        try:
            with engine.begin() as conn:
                mid, d, t1, t2, m, r1, r2 = insert_one(conn, pool)
            i += 1
            print(f"[{i}] {d} match_id={mid} | {t1} vs {t2} | {m} {r1}:{r2}")
            if delay > 0: time.sleep(delay)
        except KeyboardInterrupt:
            print("\n[stop] bye"); break
        except Exception as e:
            print("[err]", e); time.sleep(2)

if __name__ == "__main__":
    main(count=30, delay=5)  
