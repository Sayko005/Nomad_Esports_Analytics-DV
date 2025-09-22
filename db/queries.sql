
-- CS:GO Analytics SQL Queries

-- 01. Which team played the most matches in 2018?
SELECT team, COUNT(*) AS matches_played
FROM (
    SELECT match_id, team_1 AS team, match_date FROM matches
    UNION ALL
    SELECT match_id, team_2, match_date FROM matches
) t
WHERE EXTRACT(YEAR FROM match_date) = 2018
GROUP BY team
ORDER BY matches_played DESC
LIMIT 1;


-- 02. Which team won the most pistol rounds in a tournament (example event_id = 2208)?
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
WHERE winner_team IS NOT NULL
GROUP BY event_id, winner_team
ORDER BY pistol_rounds_won DESC
LIMIT 1;


-- 03. Most popular map in a given tournament ( event_id = 2208)
SELECT map, COUNT(*) AS times_played
FROM results
WHERE event_id = 2208
  AND map NOT IN ('Default', '', 'Unknown')
GROUP BY map
ORDER BY times_played DESC
LIMIT 1;


-- 04. Best player in a tournament by rating ( event_id = 2335)
SELECT player_name, team, ROUND(AVG(rating),2) AS avg_rating
FROM players_raw
WHERE event_id = 2335
GROUP BY player_name, team
ORDER BY avg_rating DESC
LIMIT 1;


-- 05. Team with the highest winrate on a map in a tournament ( event_id = 2340)
SELECT team, map,
       ROUND(100.0 * SUM(rounds_won) / SUM(result_1 + result_2), 2) AS win_rate
FROM (
    SELECT r.match_id, r.event_id, r.map, m.team_1 AS team, r.result_1 AS rounds_won, r.result_1, r.result_2
    FROM results r
    JOIN matches m ON r.match_id = m.match_id
    UNION ALL
    SELECT r.match_id, r.event_id, r.map, m.team_2, r.result_2 AS rounds_won, r.result_1, r.result_2
    FROM results r
    JOIN matches m ON r.match_id = m.match_id
) t
WHERE event_id = 2340
GROUP BY team, map
ORDER BY win_rate DESC
LIMIT 1;


-- 06. Which team has the highest average winrate across maps in a tournament ( event_id = 2345)
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
WHERE event_id = 2345
GROUP BY team
ORDER BY avg_winrate DESC
LIMIT 1;


-- 07. Number of matches per tournament
SELECT event_id, COUNT(*) AS matches_count
FROM matches
GROUP BY event_id
ORDER BY matches_count DESC;


-- 08. Best team of a given year (example 2019)
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
WHERE EXTRACT(YEAR FROM match_date) = 2019
  AND is_win = 1
GROUP BY team
ORDER BY wins DESC
LIMIT 1;



-- 09. Teams that reached overtime most often
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

-- 10. Players with the highest K/D ratio (minimum 20 maps played)
SELECT player_name, team,
       ROUND(SUM(kills)::numeric / NULLIF(SUM(deaths),0), 2) AS kd_ratio,
       COUNT(*) AS maps_played
FROM players_raw
GROUP BY player_name, team
HAVING COUNT(*) > 20
ORDER BY kd_ratio DESC
LIMIT 10;
