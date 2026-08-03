# FIFA World 2026 ELT Pipeline

## Introduction
This project implements an end-to-end data engineering pipeline that ingests raw FIFA World Cup 2026 data from the FotMob API, cleans it, transforms and refines it into analytics-ready tables, and loads it into a data warehouse for reporting.

## About the Data
This is comprehensive, raw, unprocessed JSON response data extracted from the FotMob API for every match, team, and player at the 2026 FIFA World Cup, organized into folders.
- **comp**: contains:
    - **teams_lookup.csv**: clean list of all 48 participating teams, including country ID, name, and group, to easily reference specific teams and their players if the focus of the analysis pertains to a particular team.
    - **players_lookup.csv**: clean list of all 1,243 players, including basic information like player ID, name, country, height, date of birth, club, and league, to make it easier to find files pertaining to that specific player.
    - **fixtures_lookup.csv**: clean list of all 104 scheduled matches, including information like match ID, participating teams, and if the match has started, finished, or been cancelled.
    - **comp_data.json**, consisting of general information about the competition/tournament, all fixtures, groups, and teams.
- **matches**: match data for all matches played so far, including match overview, events, stats, and participating teams and players.
- **teams**:  all 48 participating teams, including overview, squads, coaches, ranking, and ratings.
- **players**: all 1,243 players called up for the tournament, partitioned by team (team ID), including positions, historical stats, performance, clubs, and league.

**Note**: All date and time values use the UTC time zone.

I would love to see what you do with the data, so don't hesitate to share your work; tag or contact me via [LinkedIn](https://www.linkedin.com/in/binyoh-langhe-theodore) or [GitHub](https://github.com/Binyoh1).

## Architecture Overview

### Project Folder Structure

```text
.
├── src/
│   ├── common/                     # helper functions and variables
│   │   ├── api.py
│   │   ├── paths.py
│   │   └── utils.py
│   ├── extract/                    # Fetches data from FotMob API -> saves to data/raw
│   │   ├── extract_all.py          # Orchestrates api data extraction and saving raw JSON data
│   │   ├── extract_comp.py
│   │   ├── extract_matches.py
│   │   ├── extract_teams.py
│   │   └── extract_players.py
│   ├── transform/                  # Polars script: processes raw JSON -> saves to data/silver
│   │   ├── utils/                  # Python logic to build, refine, export, and load schema for processing JSON files
│   │   │   ├── schema_builder.py   # Infers and refines schema
│   │   │   └── schema_io.py        # Export and load schema (YAML format)
│   │   ├── transform_all.py        # Orchestrates polars transformations and loading silver tables to Parquet files
│   │   ├── transform_comp.py
│   │   ├── transform_matches.py
│   │   ├── transform_teams.py
│   │   └── transform_players.py
│   └── pipeline.py                 # Prefect Flow orchestrating everything
├── data/
│   ├── raw/                        # Bronze layer: raw JSON files
│   ├── schemas/                    # Inferred and refined master schemas
│   │   ├── match_schema.yaml
│   │   ├── team_schema.yaml
│   │   └── player_schema.yaml
│   ├── silver/                     # Silver layer: clean Parquet files
│   │   ├── lookup/                 # basic lookup data for matches, teams, and players
│   │   ├── reference/              # Pre-aggregated dimension/reference data for matches, teams, and players
│   │   └── facts/
│   └── gold/                       # Gold layer: final .duckdb database file
├── dbt_project/                    # dbt workspace for Gold layer models
│   ├── models/
│   │   ├── staging/                # Optional: dbt views over silver parquet files
│   │   └── marts/                  # Final aggregated tables/views for BI consumption
│   ├── dbt_project.yml             # Configured to point to data/gold/fwc2026_team_profile_analytics.duckdb
│   └── profiles.yml
├── docs/                           # Project guide, architecture diagrams, and documentation
├── requirements.txt                # Python dependencies (polars, dbt-duckdb, requests, prefect, etc.)
└── README.md                       # Project overview, description, and summary of findings
```

### Dataflow Rules
- Extract logic (`src/extract/`) only writes to and reads from `raw/` for fetching and saving raw JSON data from the FotMob API.
- Transform logic (`src/transform/`) only reads from `raw/` and writes cleaned data to `silver`Parquet files.
- Aggregation and joining logic (`dbt_project/`) only reads from `silver/` and writes analytics-ready data to `gold/`.

## To-Do
- Logic for generating and refining master schema only if loaded YAML schema from data/schemas/ fails to load JSON file(s) or isn't present.