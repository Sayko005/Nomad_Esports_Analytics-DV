from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/csgo")

queries = {
    "matches_per_event": """
        SELECT event_id, COUNT(*) AS matches_count
        FROM matches
        GROUP BY event_id
        ORDER BY matches_count DESC
        LIMIT 10;
    """,
    "top_kd_players": """
        SELECT player_name, team,
               ROUND(SUM(kills)::numeric / NULLIF(SUM(deaths),0), 2) AS kd_ratio,
               COUNT(*) AS maps_played
        FROM players_raw
        GROUP BY player_name, team
        HAVING COUNT(*) > 20
        ORDER BY kd_ratio DESC
        LIMIT 10;
    """,
    "popular_map": """
        SELECT map, COUNT(*) AS times_played
        FROM results
        GROUP BY map
        ORDER BY times_played DESC
        LIMIT 1;
    """
}

for name, sql in queries.items():
    print(f"\n--- {name} ---")
    df = pd.read_sql(sql, engine)
    print(df)

    df.to_csv(f"{name}.csv", index=False)

    
    df.to_excel(f"{name}.xlsx", index=False)

