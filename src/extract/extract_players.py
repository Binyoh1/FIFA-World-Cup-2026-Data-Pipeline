import logging
from pathlib import Path

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
    logger.info(f"Fetching player-{player_id} (team-{team_id}) data...")
    try:
        endpoint = f"{BASE_URL}/api/data/playerData?id={player_id}"
        player_json = fetch_json(endpoint, headers)
        player_path = dir_path / f"team-{team_id}/player_data_{player_id}.json"
        save_json(player_path, player_json)
        logger.info(f"Fetched and saved player-{player_id} data successfully\n")
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error player-{player_id}: {e}")
        raise
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise


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
def get_team_id(team_json: dict) -> int | None:
    team_details = team_json.get('details')
    team_id = int(team_details.get('id')) if team_details else None
    return team_id


@task
def extract_players(teams_dir: Path, players_dir: Path, headers: dict=DEFAULT_HEADERS) -> None:
    teams_json_list = list(teams_dir.glob("*.json"))
    for json_file in teams_json_list:
        try:
            # fetch and save player data
            team_id = get_team_id(load_json(json_file))
            if not team_id:
                logger.warning(f"Skipping {json_file.name}: no team_id found")
                continue
            raw_team_df = load_player_data(json_file)
            squad_list = extract_squad_list(raw_team_df)
            for player_id in squad_list['player_id']:
                player_path = players_dir / f"team-{team_id}/player_data_{player_id}.json"
                if player_path.exists():
                    continue
                fetch_and_save_player_data(player_id, team_id, headers, players_dir)
        except Exception as e:
            logger.error(f"Error processing file {json_file.name}: {e}")
            raise


if __name__ == "__main__":
    # get all saved teams
    extract_players(teams_dir, players_dir)