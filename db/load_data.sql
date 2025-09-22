

\copy economy_stage FROM 'data/economy.csv' CSV HEADER ENCODING 'LATIN1';
\copy picks_stage   FROM 'data/picks.csv'   CSV HEADER;
\copy players_stage FROM 'data/players.csv' CSV HEADER ENCODING 'LATIN1';
\copy results_stage FROM 'data/results.csv' CSV HEADER;