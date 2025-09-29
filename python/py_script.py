from sqlalchemy import create_engine
import pandas as pd

engine = create_engine("postgresql+psycopg2://postgres:admin@localhost:5432/csgo")

YEAR_MOST_MATCHES = 2018
YEAR_BEST_TEAM    = 2019
EVENT_ID_PISTOL   = 2208
EVENT_ID_POPULAR  = 2208
EVENT_ID_BEST_PLR = 2335
EVENT_ID_WINRATE_MAP = 2340
EVENT_ID_AVG_WINRATE = 2345

queries = {
    "matches_per_event_top10": """
        SELECT event_id, COUNT(*) AS matches_count
        FROM matches
        GROUP BY event_id
        ORDER BY matches_count DESC
        LIMIT 10;
    """,
    "top_kd_players_min20": """
        SELECT player_name, team,
               ROUND(SUM(kills)::numeric / NULLIF(SUM(deaths),0), 2) AS kd_ratio,
               COUNT(*) AS maps_played
        FROM players_raw
        GROUP BY player_name, team
        HAVING COUNT(*) > 20
        ORDER BY kd_ratio DESC
        LIMIT 10;
    """,
    "popular_map_overall": """
        SELECT map, COUNT(*) AS times_played
        FROM results
        GROUP BY map
        ORDER BY times_played DESC
        LIMIT 1;
    """,

    "most_matches_in_year": f"""
        SELECT team, COUNT(*) AS matches_played
        FROM (
            SELECT match_id, team_1 AS team, match_date FROM matches
            UNION ALL
            SELECT match_id, team_2, match_date FROM matches
        ) t
        WHERE EXTRACT(YEAR FROM match_date) = {YEAR_MOST_MATCHES}
        GROUP BY team
        ORDER BY matches_played DESC
        LIMIT 1;
    """,

    "most_pistol_rounds_in_event": f"""
        SELECT event_id, winner_team, COUNT(*) AS pistol_rounds_won
        FROM (
            SELECT event_id, match_id,
                   CASE WHEN "1_winner" = 1 THEN team_1
                        WHEN "1_winner" = 2 THEN team_2 END AS winner_team
            FROM economy
            UNION ALL
            SELECT event_id, match_id,
                   CASE WHEN "16_winner" = 1 THEN team_1
                        WHEN "16_winner" = 2 THEN team_2 END
            FROM economy
        ) t
        WHERE event_id = {EVENT_ID_PISTOL}
          AND winner_team IS NOT NULL
        GROUP BY event_id, winner_team
        ORDER BY pistol_rounds_won DESC
        LIMIT 1;
    """,

    "popular_map_in_event": f"""
        SELECT map, COUNT(*) AS times_played
        FROM results
        WHERE event_id = {EVENT_ID_POPULAR}
          AND map NOT IN ('Default', '', 'Unknown')
        GROUP BY map
        ORDER BY times_played DESC
        LIMIT 1;
    """,
    "best_player_by_rating_in_event": f"""
        SELECT player_name, team, ROUND(AVG(rating),2) AS avg_rating
        FROM players_raw
        WHERE event_id = {EVENT_ID_BEST_PLR}
        GROUP BY player_name, team
        ORDER BY avg_rating DESC
        LIMIT 1;
    """,

    "best_team_winrate_on_map_in_event": f"""
        SELECT team, map,
               ROUND(100.0 * SUM(rounds_won) / NULLIF(SUM(result_1 + result_2), 0), 2) AS win_rate
        FROM (
            SELECT r.match_id, r.event_id, r.map, m.team_1 AS team,
                   r.result_1 AS rounds_won, r.result_1, r.result_2
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
            UNION ALL
            SELECT r.match_id, r.event_id, r.map, m.team_2,
                   r.result_2 AS rounds_won, r.result_1, r.result_2
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
        ) t
        WHERE event_id = {EVENT_ID_WINRATE_MAP}
        GROUP BY team, map
        ORDER BY win_rate DESC
        LIMIT 1;
    """,

    "best_team_avg_winrate_in_event": f"""
        SELECT team, ROUND(AVG(win_rate),2) AS avg_winrate
        FROM (
            SELECT r.match_id, r.event_id, r.map, m.team_1 AS team,
                   ROUND(100.0 * r.result_1 / NULLIF(r.result_1 + r.result_2, 0), 2) AS win_rate
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
            UNION ALL
            SELECT r.match_id, r.event_id, r.map, m.team_2 AS team,
                   ROUND(100.0 * r.result_2 / NULLIF(r.result_1 + r.result_2, 0), 2) AS win_rate
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
        ) t
        WHERE event_id = {EVENT_ID_AVG_WINRATE}
        GROUP BY team
        ORDER BY avg_winrate DESC
        LIMIT 1;
    """,

    "matches_per_event_all": """
        SELECT event_id, COUNT(*) AS matches_count
        FROM matches
        GROUP BY event_id
        ORDER BY matches_count DESC;
    """,

    "best_team_of_year_by_wins": f"""
        SELECT team, COUNT(*) AS wins
        FROM (
            SELECT r.match_id, m.match_date, m.team_1 AS team,
                   CASE WHEN r.match_winner = 1 THEN 1 ELSE 0 END AS is_win
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
            UNION ALL
            SELECT r.match_id, m.match_date, m.team_2 AS team,
                   CASE WHEN r.match_winner = 2 THEN 1 ELSE 0 END AS is_win
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
        ) t
        WHERE EXTRACT(YEAR FROM match_date) = {YEAR_BEST_TEAM}
          AND is_win = 1
        GROUP BY team
        ORDER BY wins DESC
        LIMIT 1;
    """,

    "teams_most_overtimes_top5": """
        SELECT team, COUNT(*) AS overtime_matches
        FROM (
            SELECT r.match_id, m.team_1 AS team, (r.result_1 + r.result_2) AS total_rounds
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
            UNION ALL
            SELECT r.match_id, m.team_2, (r.result_1 + r.result_2)
            FROM results r
            JOIN matches m ON r.match_id = m.match_id
        ) t
        WHERE total_rounds > 30
        GROUP BY team
        ORDER BY overtime_matches DESC
        LIMIT 5;
    """,
}


for name, sql in queries.items():
    print(f"\n--- {name} ---")
    df = pd.read_sql(sql, engine)
    print(df)

    df.to_csv(f"{name}.csv", index=False)

    
    df.to_excel(f"{name}.xlsx", index=False)

