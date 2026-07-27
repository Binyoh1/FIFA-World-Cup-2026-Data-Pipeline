import time
import logging
from pathlib import Path
from random import randint

import requests
import polars as pl
from prefect import task

from common.api import BASE_URL, DEFAULT_HEADERS, fetch_json
from common.paths import teams_dir, players_dir
from common.utils import DELAY, save_json, load_json, setup_logging

setup_logging()

logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=DELAY)
def fetch_and_save_player_data(player_id: int, team_id: int, headers: dict, dir_path: Path) -> None:
    logging.info(f"Fetching and saving team-{team_id}'s player-{player_id} data...")    
    try:
        endpoint = f"{BASE_URL}/api/data/playerData?id={player_id}"
        player_json = fetch_json(endpoint, headers)
        
        player_path = dir_path / f"team-{team_id}/player_data_{player_id}.json"
        save_json(player_path, player_json)
        logging.info(f"Fetched and saved player-{player_id} data successfully\n")
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error player-{player_id}: {e}")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")


@task
def load_player_data(file_path: Path) -> pl.LazyFrame:
    return pl.DataFrame([load_json(file_path)], strict=False).lazy()


@task
def extract_squad_list(df: pl.LazyFrame) -> pl.DataFrame:
    squad_list = (
        df
        .select(
            pl.col('details').struct.field('id').alias('team_id'),
            pl.col('squad').struct.field('squad')
        )
        .explode('squad', empty_as_null=True)
        .select(
            pl.col('team_id'),
            pl.col('squad').struct.field('title'),
            pl.col('squad').struct.field('members'),
        )
        .explode('members', empty_as_null=True)
        .filter(
            pl.col('title') != "coach"
        )
        .select(
            pl.col('team_id'),
            pl.col('members').struct.field('id').alias('player_id'),
        )
    )
    return squad_list.collect()


@task
def get_team_ids(team_json: dict) -> int | None:
    team_details = team_json.get('details')
    team_id = int(team_details.get('id')) if team_details else None
    return team_id


if __name__ == "__main__":
    # get all saved teams
    teams_json_list = list(teams_dir.glob("*.json"))
    
    dfs = []
    for json_file in teams_json_list:
        try:
            # load team json
            raw_team_df = load_player_data(json_file)
            
            # extract squad list
            squad_list = extract_squad_list(raw_team_df)
            
            # fetch and save player data
            team_id = get_team_ids(load_json(json_file))
            for player_id in squad_list['player_id']:
                delay = randint(1, 3)
                if team_id:
                    fetch_and_save_player_data(player_id, team_id, DEFAULT_HEADERS, players_dir)
                    time.sleep(delay)
        except Exception as e:
            logging.error(f"Error reading file {json_file.name}: {e}")