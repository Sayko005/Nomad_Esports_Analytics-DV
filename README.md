# CS:GO SQL & Data Visualization Project

## 📌 Description
This project analyzes professional **Counter-Strike: Global Offensive (CS:GO)** matches using data from HLTV / Kaggle.  
The main goals are:
- Build a PostgreSQL database
- Load raw data from CSV (datasets)
- Create staging and final schemas (ETL process)
- Write and run analytical SQL queries
- Execute queries in Python and save results to CSV/Excel
- Visualize insights (ERD, tables, charts)


[CS:GO Professional Matches Dataset (Kaggle)](https://www.kaggle.com/datasets/mateusdmachado/csgo-professional-matches/data?select=economy.csv)

README.md # project description and instructions
erd.png # ERD diagram of the database
db/ # Database schema + ETL scripts
sql/ # Analytical queries
python/ # Python integration
results/ # Saved query outputs





###  1 Create PostgreSQL database
```bash
createdb csgo

### 2. Create staging schema
psql -d csgo -f db/create_staging.sql


### 3 Load CSV files into staging

###Make sure CSV files are in datasets/ and paths in db/load_data.sql match.

psql -d csgo -f db/load_data.sql


### 4 Create final schema

psql -d csgo -f db/create_final.sql


### 5 Transform data (staging → final)
 psql -d csgo -f db/transform.sql


### 6 Run analytical queries
psql -d csgo -f sql/all_queries.sql


# Build and start all services
docker compose up -d --build

# Verify
localhost:9090   # Prometheus
localhost:3000   # Grafana (admin / admin)
localhost:8000   # Custom Exporter metrics
localhost:9187   # PostgreSQL Exporter
localhost:9100   # Node Exporter





