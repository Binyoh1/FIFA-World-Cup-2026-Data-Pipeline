# FIFA World 2026 ELT Pipeline

## About the Dataset
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