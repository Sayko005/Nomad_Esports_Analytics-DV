
-- Events
INSERT INTO events(event_id)
SELECT DISTINCT event_id
FROM results_stage
ON CONFLICT (event_id) DO NOTHING;

-- Matches
INSERT INTO matches(match_id, event_id, match_date, team_1, team_2, best_of)
SELECT DISTINCT match_id, event_id, date, team_1, team_2,
       NULLIF(best_of,'o')::int
FROM economy_stage
ON CONFLICT (match_id) DO NOTHING;

-- Results
INSERT INTO results(match_id, event_id, map, result_1, result_2, map_winner, match_winner,
                    starting_ct, ct_1, t_1, ct_2, t_2,
                    rank_1, rank_2, map_wins_1, map_wins_2)
SELECT match_id, event_id, map, result_1, result_2, map_winner, match_winner,
       starting_ct, ct_1, t_1, ct_2, t_2,
       rank_1, rank_2, map_wins_1, map_wins_2
FROM results_stage
ON CONFLICT DO NOTHING;

-- Picks
INSERT INTO picks(match_id, event_id, inverted_teams, system,
                  t1_removed_1, t1_removed_2, t1_removed_3,
                  t2_removed_1, t2_removed_2, t2_removed_3,
                  t1_picked_1, t2_picked_1, left_over)
SELECT match_id, event_id, inverted_teams, system,
       t1_removed_1, t1_removed_2, t1_removed_3,
       t2_removed_1, t2_removed_2, t2_removed_3,
       t1_picked_1, t2_picked_1, left_over
FROM picks_stage
ON CONFLICT (match_id) DO NOTHING;

-- Players
INSERT INTO players_raw(match_id, event_id, match_date, player_name, team, opponent, country,
                        kills, deaths, adr, kast, kddiff, rating)
SELECT match_id, event_id, date, player_name, team, opponent, country,
       kills, deaths, adr, kast, kddiff, rating
FROM players_stage
ON CONFLICT DO NOTHING;
